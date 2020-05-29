#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>

#include <Common/typeid_cast.h>

#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <DataStreams/copyData.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int READONLY;
    extern const int ILLEGAL_COLUMN;
}


InterpreterInsertQuery::InterpreterInsertQuery(
    const ASTPtr & query_ptr_, const Context & context_, bool allow_materialized_)
    : query_ptr(query_ptr_), context(context_), allow_materialized(allow_materialized_)
{
}


StoragePtr InterpreterInsertQuery::getTable(const ASTInsertQuery & query)
{
    //使用了表函数, 如remote()表函数 SELECT count() FROM remote('example01-01-1', merge, hits) 或 INSERT INTO remote('example01-01-1', merge, hits)
    if (query.table_function)
    {
        const auto * table_function = query.table_function->as<ASTFunction>();
        const auto & factory = TableFunctionFactory::instance();
        return factory.get(table_function->name, context)->execute(query.table_function, context);
    }

    /// Into what table to write.
    return context.getTable(query.database, query.table);
}

Block InterpreterInsertQuery::getSampleBlock(const ASTInsertQuery & query, const StoragePtr & table)
{


    Block table_sample_non_materialized = table->getSampleBlockNonMaterialized();
    /// If the query does not include information about columns
    if (!query.columns)
    {
        /// Format Native ignores header and write blocks as is.
        if (query.format == "Native")
            return {};
        else
            return table_sample_non_materialized;
    }

    Block table_sample = table->getSampleBlock();
    /// Form the block based on the column names from the query
    Block res;
    for (const auto & identifier : query.columns->children)
    {
        std::string current_name = identifier->getColumnName();

        /// The table does not have a column with that name
        if (!table_sample.has(current_name))
            throw Exception("No such column " + current_name + " in table " + query.table, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!allow_materialized && !table_sample_non_materialized.has(current_name))
            throw Exception("Cannot insert column " + current_name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);

        res.insert(ColumnWithTypeAndName(table_sample.getByName(current_name).type, current_name));
    }
    return res;
}


BlockIO InterpreterInsertQuery::execute()
{
    const auto & query = query_ptr->as<ASTInsertQuery &>();
    checkAccess(query);
    StoragePtr table = getTable(query);

    //对表结构加锁
    auto table_lock = table->lockStructureForShare(true, context.getCurrentQueryId());

    /// We create a pipeline of several streams, into which we will write data.
    /// 创建一个由多个流组成的管道, 将数据写入其中.
    BlockOutputStreamPtr out;

    //构建PushingToViewsBlockOutputStream
    out = std::make_shared<PushingToViewsBlockOutputStream>(query.database, query.table, table, context, query_ptr, query.no_destination);

    /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
    /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
    // sync INSERT的时候不要squash blocks, 这会导致client和server的双重缓存. client的缓存可能会导致超时(尤其是在大blocks的情况下)
    // insert_distributed_sync (false/true, 默认false): If setting is enabled, insert query into distributed waits until data will be sent to all nodes in cluster
    // insert_distributed_timeout (默认0): Timeout for insert query into distributed. Setting is used only with insert_distributed_sync enabled. Zero value means no timeout.
    /// 如果insert_distributed_sync=true且table->isRemote()=true,
    /// 则是sync INSERT且表是远程表, 则不会走下面的逻辑, 不会创建SquashingBlockOutputStream(这个stream的作用是把小的block合并成大的block)
    if (!(context.getSettingsRef().insert_distributed_sync && table->isRemote()))
    {
        out = std::make_shared<SquashingBlockOutputStream>(
            out, table->getSampleBlock(), context.getSettingsRef().min_insert_block_size_rows, context.getSettingsRef().min_insert_block_size_bytes);
    }
    auto query_sample_block = getSampleBlock(query, table);

    /// Actually we don't know structure of input blocks from query/table, because some clients break insertion protocol (columns != header)
    /// 实际上, 我们不知道query/table中输入块的结构, 因为有些客户端破坏了插入协议（columns != header）
    out = std::make_shared<AddingDefaultBlockOutputStream>(
        out, query_sample_block, table->getSampleBlock(), table->getColumns().getDefaults(), context);

    auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
    out_wrapper->setProcessListElement(context.getProcessListElement());
    out = std::move(out_wrapper);

    BlockIO res;
    res.out = std::move(out);
    //res.out 多个流组成的管道: CountingBlockOutputStream -> AddingDefaultBlockOutputStream -> (SquashingBlockOutputStream) -> PushingToViewsBlockOutputStream ->  .....

    /// What type of query: INSERT or INSERT SELECT?
    if (query.select) //如果是INSERT SELECT, 则query.select不为空, 执行下面的逻辑
    {
        /// Passing 1 as subquery_depth will disable limiting size of intermediate result.
        // 将子查询深度限定为1, 将禁用限制中间结果的大小 (中间结果的大小将不受限制)
        InterpreterSelectWithUnionQuery interpreter_select{query.select, context, SelectQueryOptions(QueryProcessingStage::Complete, 1)};

        res.in = interpreter_select.execute().in;

        // 构造ConvertingBlockInputStream, 将res.in中的block的结构转化成res.out中block的结构, 这样就可以直接将res.in中的block stream写入底层表了
        res.in = std::make_shared<ConvertingBlockInputStream>(context, res.in, res.out->getHeader(), ConvertingBlockInputStream::MatchColumnsMode::Position);
        // 构造NullAndDoCopyBlockInputStream, 在第一次尝试读取时, 会将数据从res.in复制到res.out;
        res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, res.out);

        res.out = nullptr;

        if (!allow_materialized)
        {
            Block in_header = res.in->getHeader();
            for (const auto & column : table->getColumns()) //不能直接写物化列
                if (column.default_desc.kind == ColumnDefaultKind::Materialized && in_header.has(column.name))
                    throw Exception("Cannot insert column " + column.name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        }
    }
    else if (query.data && !query.has_tail) /// can execute without additional data 数据包含在SQL中, 可以在没有附加数据的情况下执行
    {
        // 构造InputStreamFromASTInsertQuery,
        //对于数据包含在SQL中的INSERT SQL, 需要插入的数据的结构可以直接存储在INSERT AST中, 具体的数据存储在input_buffer_tail_part中
        res.in = std::make_shared<InputStreamFromASTInsertQuery>(query_ptr, nullptr, query_sample_block, context);
        // 构造NullAndDoCopyBlockInputStream, 在第一次尝试读取时, 会将数据从res.in复制到res.out;
        res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, res.out);
        res.out = nullptr;
    }
    /// 对于除 INSERT SELECT 和 data不包含在INSERT SQL中 之外的情况, res.in = nullptr; res.out = std::move(out);

    return res;
}


void InterpreterInsertQuery::checkAccess(const ASTInsertQuery & query)
{
    const Settings & settings = context.getSettingsRef();
    auto readonly = settings.readonly;

    if (!readonly || (query.database.empty() && context.tryGetExternalTable(query.table) && readonly >= 2))
    {
        return;
    }

    throw Exception("Cannot insert into table in readonly mode", ErrorCodes::READONLY);
}

std::pair<String, String> InterpreterInsertQuery::getDatabaseTable() const
{
    const auto & query = query_ptr->as<ASTInsertQuery &>();
    return {query.database, query.table};
}

}
