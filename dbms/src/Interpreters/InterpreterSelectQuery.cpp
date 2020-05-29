#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/LimitByBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/MergingAggregatedBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/DistinctBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/TotalsHavingBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/RollupBlockInputStream.h>
#include <DataStreams/CubeBlockInputStream.h>
#include <DataStreams/ConvertColumnLowCardinalityToFullBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/JoinToSubqueryTransformVisitor.h>
#include <Interpreters/CrossToInnerJoinVisitor.h>

#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Core/Field.h>
#include <Core/Types.h>
#include <Columns/Collator.h>
#include <Common/typeid_cast.h>
#include <Parsers/queryToString.h>
#include <ext/map.h>
#include <memory>


namespace DB {

    namespace ErrorCodes {
        extern const int TOO_DEEP_SUBQUERIES;
        extern const int THERE_IS_NO_COLUMN;
        extern const int SAMPLING_NOT_SUPPORTED;
        extern const int ILLEGAL_FINAL;
        extern const int ILLEGAL_PREWHERE;
        extern const int TOO_MANY_COLUMNS;
        extern const int LOGICAL_ERROR;
        extern const int NOT_IMPLEMENTED;
        extern const int PARAMETER_OUT_OF_BOUND;
        extern const int ARGUMENT_OUT_OF_BOUND;
        extern const int INVALID_LIMIT_EXPRESSION;
    }

    namespace {

/// Assumes `storage` is set and the table filter is not empty.
        String generateFilterActions(ExpressionActionsPtr &actions, const StoragePtr &storage, const Context &context,
                                     const Names &prerequisite_columns = {}) {
            const auto &db_name = storage->getDatabaseName();
            const auto &table_name = storage->getTableName();
            const auto &filter_str = context.getUserProperty(db_name, table_name, "filter");

            /// TODO: implement some AST builders for this kind of stuff
            ASTPtr query_ast = std::make_shared<ASTSelectQuery>();
            auto *select_ast = query_ast->as<ASTSelectQuery>();

            select_ast->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
            auto expr_list = select_ast->select();

            auto parseExpression = [](const String &expr) {
                ParserExpression expr_parser;
                return parseQuery(expr_parser, expr, 0);
            };

            // The first column is our filter expression.
            expr_list->children.push_back(parseExpression(filter_str));

            /// Keep columns that are required after the filter actions.
            for (const auto &column_str : prerequisite_columns)
                expr_list->children.push_back(parseExpression(column_str));

            select_ast->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
            auto tables = select_ast->tables();
            auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_expr = std::make_shared<ASTTableExpression>();
            tables->children.push_back(tables_elem);
            tables_elem->table_expression = table_expr;
            tables_elem->children.push_back(table_expr);
            table_expr->database_and_table_name = createTableIdentifier(db_name, table_name);
            table_expr->children.push_back(table_expr->database_and_table_name);

            /// Using separate expression analyzer to prevent any possible alias injection
            auto syntax_result = SyntaxAnalyzer(context).analyze(query_ast, storage->getColumns().getAllPhysical());
            ExpressionAnalyzer analyzer(query_ast, syntax_result, context);
            ExpressionActionsChain new_chain(context);
            analyzer.appendSelect(new_chain, false);
            actions = new_chain.getLastActions();

            return expr_list->children.at(0)->getColumnName();
        }

    } // namespace

    InterpreterSelectQuery::InterpreterSelectQuery(
            const ASTPtr &query_ptr_,
            const Context &context_,
            const SelectQueryOptions &options,
            const Names &required_result_column_names)
            //调用这个这个方法
            : InterpreterSelectQuery(query_ptr_, context_, nullptr, nullptr, options, required_result_column_names) {
    }

    InterpreterSelectQuery::InterpreterSelectQuery(
            const ASTPtr &query_ptr_,
            const Context &context_,
            const BlockInputStreamPtr &input_,
            const SelectQueryOptions &options)
            : InterpreterSelectQuery(query_ptr_, context_, input_, nullptr, options.copy().noSubquery()) {}

    InterpreterSelectQuery::InterpreterSelectQuery(
            const ASTPtr &query_ptr_,
            const Context &context_,
            const StoragePtr &storage_,
            const SelectQueryOptions &options)
            : InterpreterSelectQuery(query_ptr_, context_, nullptr, storage_, options.copy().noSubquery()) {}

    InterpreterSelectQuery::~InterpreterSelectQuery() = default;


/** There are no limits on the maximum size of the result for the subquery.
  *  Since the result of the query is not the result of the entire query.
  */
    static Context getSubqueryContext(const Context &context) {
        Context subquery_context = context;
        Settings subquery_settings = context.getSettings();
        subquery_settings.max_result_rows = 0;
        subquery_settings.max_result_bytes = 0;
        /// The calculation of extremes does not make sense and is not necessary (if you do it, then the extremes of the subquery can be taken for whole query).
        subquery_settings.extremes = 0;
        subquery_context.setSettings(subquery_settings);
        return subquery_context;
    }

    InterpreterSelectQuery::InterpreterSelectQuery(
            const ASTPtr &query_ptr_,
            const Context &context_,
            const BlockInputStreamPtr &input_,
            const StoragePtr &storage_,
            const SelectQueryOptions &options_,
            const Names &required_result_column_names)
            : options(options_)
            /// NOTE: the query almost always should be cloned because it will be modified during analysis.   modify_inplace=false, 所以一般都会clone一份query_ptr
            , query_ptr(options.modify_inplace ? query_ptr_ : query_ptr_->clone()), context(context_),
              storage(storage_), input(input_), log(&Logger::get("InterpreterSelectQuery")) {
        initSettings();
        const Settings &settings = context.getSettingsRef();

        if (settings.max_subquery_depth && options.subquery_depth > settings.max_subquery_depth)
            throw Exception("Too deep subqueries. Maximum: " + settings.max_subquery_depth.toString(),
                            ErrorCodes::TOO_DEEP_SUBQUERIES);

        // if true, Convert CROSS JOIN to INNER JOIN if possible
        // 通过构建*Visitor并调用visit()方法, 修改语法树, 将 cross join 改写成 inner join
        if (settings.allow_experimental_cross_to_join_conversion) {
            CrossToInnerJoinVisitor::Data cross_to_inner;
            CrossToInnerJoinVisitor(cross_to_inner).visit(query_ptr);
        }

        //if true, Emulate multiple joins using subselects          //用 子查询 代替 多个join
        if (settings.allow_experimental_multiple_joins_emulation) {
            JoinToSubqueryTransformVisitor::Data join_to_subs_data{context};
            JoinToSubqueryTransformVisitor(join_to_subs_data).visit(query_ptr);
        }

        max_streams = settings.max_threads; //The maximum number of threads to execute the request. By default, it is determined automatically.
        auto &query = getSelectQuery();

        //提取 表 表达式(库名.表名 或 表函数, 还会区分是否是子查询)
        ASTPtr table_expression = extractTableExpression(query, 0);

        bool is_table_func = false;
        bool is_subquery = false;
        if (table_expression) {
            is_table_func = table_expression->as<ASTFunction>();
            is_subquery = table_expression->as<ASTSelectWithUnionQuery>();
        }

        if (input) {
            /// Read from prepared input.
            source_header = input->getHeader();
        } else if (is_subquery) {
            /// Read from subquery.
            interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
                    table_expression, getSubqueryContext(context), options.subquery(), required_columns);

            source_header = interpreter_subquery->getSampleBlock();
        } else if (!storage) {
            if (is_table_func) {
                /// Read from table function.
                storage = context.getQueryContext().executeTableFunction(table_expression);
            } else {
                /// Read from table. Even without table expression (implicit SELECT ... FROM system.one).
                String database_name;
                String table_name;

                getDatabaseAndTableNames(database_name, table_name);

                storage = context.getTable(database_name, table_name);
            }
        }

        if (storage) //对于从底层存储引擎中读取数据的query, 对当前表结构加锁(共享锁)(ReadLock)
            table_lock = storage->lockStructureForShare(false, context.getCurrentQueryId());

        //AST语法分析结果, 先构建SyntaxAnalyzer, 再调用其analyze()方法. 进行一些列的优化
        syntax_analyzer_result = SyntaxAnalyzer(context, options).analyze(query_ptr, source_header.getNamesAndTypesList(), required_result_column_names, storage);

        //将语法树的一个表达式转换为执行的一系列操作
        query_analyzer = std::make_unique<ExpressionAnalyzer>(query_ptr, syntax_analyzer_result, context, NamesAndTypesList(), NameSet(required_result_column_names.begin(), required_result_column_names.end()), options.subquery_depth, !options.only_analyze);

        if (!options.only_analyze) {
            if (query.sample_size() && (input || !storage || !storage->supportsSampling()))
                throw Exception("Illegal SAMPLE: table doesn't support sampling", ErrorCodes::SAMPLING_NOT_SUPPORTED);

            if (query.final() && (input || !storage || !storage->supportsFinal()))
                throw Exception((!input && storage) ? "Storage " + storage->getName() + " doesn't support FINAL"
                                                    : "Illegal FINAL", ErrorCodes::ILLEGAL_FINAL);

            if (query.prewhere() && (input || !storage || !storage->supportsPrewhere()))
                throw Exception((!input && storage) ? "Storage " + storage->getName() + " doesn't support PREWHERE"
                                                    : "Illegal PREWHERE", ErrorCodes::ILLEGAL_PREWHERE);

            /// Save the new temporary tables in the query context
            // 将产生的临时表加到query上下文中
            for (const auto &it : query_analyzer->getExternalTables())
                if (!context.tryGetExternalTable(it.first))
                    context.addExternalTable(it.first, it.second);
        }

        if (!options.only_analyze || options.modify_inplace) {
            if (query_analyzer->isRewriteSubqueriesPredicate()) {
                /// remake interpreter_subquery when PredicateOptimizer rewrites subqueries and main table is subquery
                // 当PredicateOptimizer重写子查询且主表为子查询时, 重新生成interpreter_subquery
                if (is_subquery)
                    interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
                            table_expression,
                            getSubqueryContext(context),
                            options.subquery(),
                            required_columns);
            }
        }

        if (interpreter_subquery) {
            /// If there is an aggregation in the outer query, WITH TOTALS is ignored in the subquery.
            /// 如果外层查询中存在聚合, 则子查询中将忽略WITH TOTALS这个关键字
            if (query_analyzer->hasAggregation())
                interpreter_subquery->ignoreWithTotals();
        }

        required_columns = query_analyzer->getRequiredSourceColumns();

        if (storage)
            source_header = storage->getSampleBlockForColumns(required_columns);

        /// Calculate structure of the result.
        {
            Pipeline pipeline;
            // 这里的这个executeImpl()方法使用来确定结果数据的结构(表头)的, 此时dry_run = true. 注意: execute()方法中还有一个executeImpl()方法的调用, 此时dry_run = false
            executeImpl(pipeline, nullptr, true);
            result_header = pipeline.firstStream()->getHeader();
        }
    }


    void InterpreterSelectQuery::getDatabaseAndTableNames(String &database_name, String &table_name) {
        if (auto db_and_table = getDatabaseAndTable(getSelectQuery(), 0)) {
            table_name = db_and_table->table;
            database_name = db_and_table->database;

            /// If the database is not specified - use the current database.
            if (database_name.empty() && !context.tryGetTable("", table_name))
                database_name = context.getCurrentDatabase();
        } else /// If the table is not specified - use the table `system.one`.
        {
            database_name = "system";
            table_name = "one";
        }
    }


    Block InterpreterSelectQuery::getSampleBlock() {
        return result_header;
    }


    BlockIO InterpreterSelectQuery::execute()
    {
        Pipeline pipeline;
        executeImpl(pipeline, input, options.only_analyze);//SELECT语句的具体的执行实现
        executeUnion(pipeline);//执行UNION操作

        BlockIO res;
        res.in = pipeline.firstStream();//firstStream中保存的应该是表头(就是列名), 具体的作用需要明确一下
        return res;
    }

    BlockInputStreams InterpreterSelectQuery::executeWithMultipleStreams()
    {
        Pipeline pipeline;
        executeImpl(pipeline, input, options.only_analyze);
        return pipeline.streams;
    }

    InterpreterSelectQuery::AnalysisResult
    InterpreterSelectQuery::analyzeExpressions(QueryProcessingStage::Enum from_stage, bool dry_run, const FilterInfoPtr &filter_info)
    {
        AnalysisResult res;

        //用于确定当前server上的阶段处于执行计划的哪个阶段
        /// Do I need to perform the first part of the pipeline - running on remote servers during distributed processing.
        // 是执行计划的第一阶段吗？ first_stage:分布式查询期间在remote servers上运行  (from_stage = FetchColumns && to_stage >= WithMergeableState)
        res.first_stage = from_stage < QueryProcessingStage::WithMergeableState
                          && options.to_stage >= QueryProcessingStage::WithMergeableState;
        /// Do I need to execute the second part of the pipeline - running on the initiating server during distributed processing.
        // 是执行计划的第二阶段吗？ second_stage:分布式查询期间在initiating server上运行  (from_stage <= WithMergeableState && to_stage = Complete)
        res.second_stage = from_stage <= QueryProcessingStage::WithMergeableState
                           && options.to_stage > QueryProcessingStage::WithMergeableState;

        /** First we compose a chain of actions and remember the necessary steps from it.
          *  Regardless of from_stage and to_stage, we will compose a complete sequence of actions to perform optimization and
          *  throw out unnecessary columns based on the entire query.
          *  In unnecessary parts of the query, we will not execute subqueries.
          */

        bool has_filter = false;
        bool has_prewhere = false;
        bool has_where = false;
        size_t where_step_num;

        //先定义了finalizeChain()这个方法, 供后面调用. 用于将所有的step连贯起来
        auto finalizeChain = [&](ExpressionActionsChain &chain) {
            chain.finalize();

            if (has_prewhere) {
                const ExpressionActionsChain::Step &step = chain.steps.at(0);
                res.prewhere_info->remove_prewhere_column = step.can_remove_required_output.at(0);

                Names columns_to_remove;
                for (size_t i = 1; i < step.required_output.size(); ++i) {
                    if (step.can_remove_required_output[i])
                        columns_to_remove.push_back(step.required_output[i]);
                }

                if (!columns_to_remove.empty()) {
                    auto columns = res.prewhere_info->prewhere_actions->getSampleBlock().getNamesAndTypesList();
                    ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(columns, context);
                    for (const auto &column : columns_to_remove)
                        actions->add(ExpressionAction::removeColumn(column));

                    res.prewhere_info->remove_columns_actions = std::move(actions);
                }

                res.columns_to_remove_after_prewhere = std::move(columns_to_remove);
            } else if (has_filter) {
                /// Can't have prewhere and filter set simultaneously
                res.filter_info->do_remove_column = chain.steps.at(0).can_remove_required_output.at(0);
            }
            if (has_where)
                res.remove_where_filter = chain.steps.at(where_step_num).can_remove_required_output.at(0);

            has_filter = has_prewhere = has_where = false;

            chain.clear();
        };

        {
            ExpressionActionsChain chain(context);
            auto &query = getSelectQuery();

            Names additional_required_columns_after_prewhere;

            if (storage && query.sample_size()) {
                Names columns_for_sampling = storage->getColumnsRequiredForSampling();
                additional_required_columns_after_prewhere.insert(additional_required_columns_after_prewhere.end(),
                                                                  columns_for_sampling.begin(),
                                                                  columns_for_sampling.end());
            }

            if (storage && query.final()) {
                Names columns_for_final = storage->getColumnsRequiredForFinal();
                additional_required_columns_after_prewhere.insert(additional_required_columns_after_prewhere.end(),
                                                                  columns_for_final.begin(), columns_for_final.end());
            }

            if (storage && context.hasUserProperty(storage->getDatabaseName(), storage->getTableName(), "filter")) {
                has_filter = true;

                /// XXX: aggregated copy-paste from ExpressionAnalyzer::appendSmth()
                if (chain.steps.empty()) {
                    chain.steps.emplace_back(std::make_shared<ExpressionActions>(source_columns, context));
                }
                ExpressionActionsChain::Step &step = chain.steps.back();

                // FIXME: assert(filter_info);
                res.filter_info = filter_info;
                step.actions = filter_info->actions;
                step.required_output.push_back(res.filter_info->column_name);
                step.can_remove_required_output = {true};

                chain.addStep();
            }

            if (query_analyzer->appendPrewhere(chain, !res.first_stage, additional_required_columns_after_prewhere)) {
                has_prewhere = true;

                res.prewhere_info = std::make_shared<PrewhereInfo>(
                        chain.steps.front().actions, query.prewhere()->getColumnName());

                chain.addStep();
            }

            res.need_aggregate = query_analyzer->hasAggregation();

            query_analyzer->appendArrayJoin(chain, dry_run || !res.first_stage);

            if (query_analyzer->appendJoin(chain, dry_run || !res.first_stage)) {
                res.before_join = chain.getLastActions();
                if (!res.hasJoin())
                    throw Exception("No expected JOIN", ErrorCodes::LOGICAL_ERROR);
                chain.addStep();
            }

            if (query_analyzer->appendWhere(chain, dry_run || !res.first_stage)) {
                where_step_num = chain.steps.size() - 1;
                has_where = res.has_where = true;
                res.before_where = chain.getLastActions();
                chain.addStep();
            }

            if (res.need_aggregate) {
                query_analyzer->appendGroupBy(chain, dry_run || !res.first_stage);
                query_analyzer->appendAggregateFunctionsArguments(chain, dry_run || !res.first_stage);
                res.before_aggregation = chain.getLastActions();

                finalizeChain(chain);

                if (query_analyzer->appendHaving(chain, dry_run || !res.second_stage)) {
                    res.has_having = true;
                    res.before_having = chain.getLastActions();
                    chain.addStep();
                }
            }

            /// If there is aggregation, we execute expressions in SELECT and ORDER BY on the initiating server, otherwise on the source servers.
            //重点//如果有聚合操作, 则在启动服务器上对SELECT和ORDER BY执行action expressions, 否则在源服务器上执行
            query_analyzer->appendSelect(chain, dry_run || (res.need_aggregate ? !res.second_stage : !res.first_stage));

            res.selected_columns = chain.getLastStep().required_output;
            res.has_order_by = query_analyzer->appendOrderBy(chain, dry_run || (res.need_aggregate ? !res.second_stage
                                                                                                   : !res.first_stage));
            res.before_order_and_select = chain.getLastActions();
            chain.addStep();

            if (query_analyzer->appendLimitBy(chain, dry_run || !res.second_stage)) {
                res.has_limit_by = true;
                res.before_limit_by = chain.getLastActions();
                chain.addStep();
            }

            query_analyzer->appendProjectResult(chain);
            res.final_projection = chain.getLastActions();

            finalizeChain(chain);
        }

        /// Before executing WHERE and HAVING, remove the extra columns from the block (mostly the aggregation keys).
        //在执行WHERE和HAVING之前, 删除多余的列(只保留需要select出来的列)
        //prepend前置 Project投影/映射 Input输入
        if (res.filter_info)
            res.filter_info->actions->prependProjectInput();
        if (res.has_where)
            res.before_where->prependProjectInput();
        if (res.has_having)
            res.before_having->prependProjectInput();

        res.subqueries_for_sets = query_analyzer->getSubqueriesForSets();

        /// Check that PREWHERE doesn't contain unusual actions. Unusual actions are that can change number of rows.
        if (res.prewhere_info) {
            auto check_actions = [](const ExpressionActionsPtr &actions) {
                if (actions)
                    for (const auto &action : actions->getActions())
                        if (action.type == ExpressionAction::Type::JOIN ||
                            action.type == ExpressionAction::Type::ARRAY_JOIN)
                            throw Exception("PREWHERE cannot contain ARRAY JOIN or JOIN action",
                                            ErrorCodes::ILLEGAL_PREWHERE);
            };

            check_actions(res.prewhere_info->prewhere_actions);
            check_actions(res.prewhere_info->alias_actions);
            check_actions(res.prewhere_info->remove_columns_actions);
        }

        return res;
    }


    //执行的具体实现(重点)
    void InterpreterSelectQuery::executeImpl(Pipeline &pipeline, const BlockInputStreamPtr &prepared_input, bool dry_run)
    {
        /** Streams of data.
         *  When the query is executed in parallel, we have several data streams.
         *    If there is no GROUP BY, then perform all operations before ORDER BY and LIMIT in parallel, then
         *        if there is an ORDER BY, then glue the streams using UnionBlockInputStream, and then MergeSortingBlockInputStream,
         *        if not, then glue it using UnionBlockInputStream,
         *        then apply LIMIT.
         *    If there is GROUP BY, then we will perform all operations up to GROUP BY, inclusive, in parallel;
         *
         *  a parallel GROUP BY will glue streams into one,
         *  then perform the remaining operations with one resulting stream.
         */
        // 并行查询的时候, 会存在多个数据流.
        // 如果不存在GROUP BY, 则ORDER BY 和 LIMIT 之前的操作都是并行的
        //    如果存在ORDER BY, 先使用UnionBlockInputStream将多个流聚合成一个流, 然后MergeSortingBlockInputStream(即进行排序);
        //    如果不存在ORDER BY, 仅使用UnionBlockInputStream将多个流聚合成一个流;
        //    然后执行LIMIT操作;
        // 如果存在GROUP BY, GROUP BY之前(包括GROUP BY)的所有操作都是并行的, 然后并行的GROUP BY操作将多个流聚合成为一个, 再进行后续的处理
        /// Now we will compose block streams that perform the necessary actions.
        auto &query = getSelectQuery();
        const Settings &settings = context.getSettingsRef();

        //SELECT query执行过程所处的阶段, FetchColumns阶段是要从底层文件中将数据读入Block中
        QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;

        /// PREWHERE optimization

        /// Turn off, if the table filter is applied.  如果使用了table filter, 不走这块逻辑
        // 使用了table filter时, 不进行优化, 即不会将WHERE中的条件移动到PREWHERE中. (table filter是根据user.xml设置的，具体作用不详)
        if (storage && !context.hasUserProperty(storage->getDatabaseName(), storage->getTableName(), "filter")) {
            /// 代码运行到这里表示: 是从表中读取数据 且 没有设置filter属性, 可以将WHERE中的条件移动到PREWHERE中进行优化
            if (!dry_run)
                from_stage = storage->getQueryProcessingStage(context);

            query_analyzer->makeSetsForIndex();

            auto optimize_prewhere = [&](auto &merge_tree) {
                SelectQueryInfo query_info;
                query_info.query = query_ptr;
                query_info.syntax_analyzer_result = syntax_analyzer_result;
                query_info.sets = query_analyzer->getPreparedSets();

                /// Try transferring some condition from WHERE to PREWHERE if enabled and viable
                // 允许优化 且 where条件存在 且 prewhere条件不存在 且 query.final()=false 时会进行优化
                // query.final()=false可能指table_expression为空或modifier=false
                if (settings.optimize_move_to_prewhere && query.where() && !query.prewhere() && !query.final())
                    MergeTreeWhereOptimizer{query_info, context, merge_tree, query_analyzer->getRequiredSourceColumns(), log};
            };

            if (const MergeTreeData *merge_tree_data = dynamic_cast<const MergeTreeData *>(storage.get()))
                optimize_prewhere(*merge_tree_data);
        }

        AnalysisResult expressions;
        FilterInfoPtr filter_info;

        /// We need proper `source_header` for `NullBlockInputStream` in dry-run.
        if (storage && context.hasUserProperty(storage->getDatabaseName(), storage->getTableName(), "filter")) {
            /// 代码运行到这里表示: 是从表中读取数据 且 设置了filter属性
            filter_info = std::make_shared<FilterInfo>();
            filter_info->column_name = generateFilterActions(filter_info->actions, storage, context, required_columns);
            source_header = storage->getSampleBlockForColumns(filter_info->actions->getRequiredColumns());
        }

        //dry_run = true, 表示用来获取表头结构; dry_run = false, 表示用来获取数据
        if (dry_run) {
            //dry_run = true, 表示用来获取表头结构. 不需要实际的数据, 所以在这里构造一个NullBlockInputStream
            pipeline.streams.emplace_back(std::make_shared<NullBlockInputStream>(source_header));

            //分析SQL中的'过滤条件'
            expressions = analyzeExpressions(QueryProcessingStage::FetchColumns, true, filter_info);

            //如果表是按行级安全表达式筛选的，则不支持PREWHERE
            if (storage && expressions.filter_info && expressions.prewhere_info)
                throw Exception("PREWHERE is not supported if the table is filtered by row-level security expression", ErrorCodes::ILLEGAL_PREWHERE);

            if (expressions.prewhere_info)
                pipeline.streams.back() = std::make_shared<FilterBlockInputStream>(
                        pipeline.streams.back(), expressions.prewhere_info->prewhere_actions,
                        expressions.prewhere_info->prewhere_column_name,
                        expressions.prewhere_info->remove_prewhere_column);
        } else {
            if (prepared_input)
                pipeline.streams.push_back(prepared_input);

            //分析SQL中的'过滤条件'
            expressions = analyzeExpressions(from_stage, false, filter_info);

            //不支持分布式表嵌套(from_stage=WithMergeableState && to_stage=WithMergeableState)
            if (from_stage == QueryProcessingStage::WithMergeableState && options.to_stage == QueryProcessingStage::WithMergeableState)
                throw Exception("Distributed on Distributed is not supported", ErrorCodes::NOT_IMPLEMENTED);

            //按行级安全性表达式筛选的表不支持PREWHERE子句
            if (storage && expressions.filter_info && expressions.prewhere_info)
                throw Exception("PREWHERE is not supported if the table is filtered by row-level security expression", ErrorCodes::ILLEGAL_PREWHERE);

            /** Read the data from Storage.
              * from_stage - to what stage the request was completed in Storage.
              */
            // 重点实现. 构造从底层存储引擎Storage中读取数据的Stream.
            // pipeline保存着具体的执行计划(就是在确定pipeline中的各级stream)
            executeFetchColumns(from_stage, pipeline, expressions.prewhere_info, expressions.columns_to_remove_after_prewhere);

            LOG_TRACE(log, QueryProcessingStage::toString(from_stage) << " -> " << QueryProcessingStage::toString(
                    options.to_stage));
        }

        //如果to_stage是WithMergeableState或Complete, 需要继续执行下面的操作
        //如果to_stage是FetchColumns, 则不需要继续执行
        if (options.to_stage > QueryProcessingStage::FetchColumns) {
            /// Do I need to aggregate in a separate row rows that have not passed max_rows_to_group_by.
            //如果没有指定max_rows_to_group_by这个参数, 是否需要将多余的行聚合成一行
            bool aggregate_overflow_row =
                    expressions.need_aggregate &&
                    query.group_by_with_totals &&
                    settings.max_rows_to_group_by &&
                    settings.group_by_overflow_mode == OverflowMode::ANY &&
                    settings.totals_mode != TotalsMode::AFTER_HAVING_EXCLUSIVE;

            /// Do I need to immediately finalize the aggregate functions after the aggregation?
            //聚合后是否需要立即完成聚合函数？即是否需要保留聚合的中间状态？
            bool aggregate_final =
                    expressions.need_aggregate &&
                    options.to_stage > QueryProcessingStage::WithMergeableState &&  //to_stage = Complete
                    !query.group_by_with_totals && !query.group_by_with_rollup && !query.group_by_with_cube; //是否包含GROUP BY ... WITH CUBE、GROUP BY ... WITH ROLLUP、GROUP BY ... WITH TOTALS

            //执行计划的第一阶段(分布式查询期间在远程服务器上运行)
            if (expressions.first_stage) {
                if (expressions.filter_info) {//如果有filter_info, 则需要先构造FilterBlockInputStream
                    pipeline.transform([&](auto &stream) {
                        stream = std::make_shared<FilterBlockInputStream>(
                                stream,
                                expressions.filter_info->actions,
                                expressions.filter_info->column_name,
                                expressions.filter_info->do_remove_column);
                    });
                }

                //处理Join操作
                if (expressions.hasJoin()) {
                    const auto &join = query.join()->table_join->as<ASTTableJoin &>();
                    if (isRightOrFull(join.kind)) //RIGHT JOIN 或 FULL JOIN.  createStreamWithNonJoinedDataIfFullOrRightJoin() 重点看下
                        pipeline.stream_with_non_joined_data = expressions.before_join->createStreamWithNonJoinedDataIfFullOrRightJoin(
                                pipeline.firstStream()->getHeader(), settings.max_block_size);

                    for (auto &stream : pipeline.streams)   /// Applies to all sources except stream_with_non_joined_data.
                        stream = std::make_shared<ExpressionBlockInputStream>(stream, expressions.before_join);
                }

                //处理where操作
                if (expressions.has_where)
                    executeWhere(pipeline, expressions.before_where, expressions.remove_where_filter);

                //处理聚合操作
                if (expressions.need_aggregate)
                    executeAggregation(pipeline, expressions.before_aggregation, aggregate_overflow_row, aggregate_final);
                else {
                    //不包含聚合操作时需要进行的操作
                    //executeExpression是个重点方法, 仔细看一下
                    executeExpression(pipeline, expressions.before_order_and_select);
                    //处理Distinct操作
                    executeDistinct(pipeline, true, expressions.selected_columns);
                }

                /** For distributed query processing, if no GROUP, HAVING set, but there is an ORDER or LIMIT,
                  *  then we will perform the preliminary sorting and LIMIT on the remote server.
                  */
                //  对于分布式查询, 如果不包含GROUP和HAVING操作, 但是包含ORDER或LIMIT操作,
                //  那么我们将在远程服务器上执行初步的sorting和limit
                if (!expressions.second_stage && !expressions.need_aggregate && !expressions.has_having) {
                    if (expressions.has_order_by)
                        executeOrder(pipeline);

                    if (expressions.has_order_by && query.limitLength())
                        executeDistinct(pipeline, false, expressions.selected_columns);

                    if (expressions.has_limit_by) {
                        executeExpression(pipeline, expressions.before_limit_by);
                        executeLimitBy(pipeline);
                    }

                    if (query.limitLength())
                        executePreLimit(pipeline);
                }

                // If there is no global subqueries, we can run subqueries only when receive them on server.
                if (!query_analyzer->hasGlobalSubqueries() && !expressions.subqueries_for_sets.empty())
                    executeSubqueriesInSetsAndJoins(pipeline, expressions.subqueries_for_sets);
            }

            //执行计划的第二阶段(分布式查询期间在启动服务器上运行)
            if (expressions.second_stage) {
                bool need_second_distinct_pass = false;
                bool need_merge_streams = false;

                //包含聚合操作
                if (expressions.need_aggregate) {
                    /// If you need to combine aggregated results from multiple servers
                    if (!expressions.first_stage)
                        executeMergeAggregated(pipeline, aggregate_overflow_row, aggregate_final);

                    if (!aggregate_final) {
                        if (query.group_by_with_totals) {
                            bool final = !query.group_by_with_rollup && !query.group_by_with_cube;
                            executeTotalsAndHaving(pipeline, expressions.has_having, expressions.before_having,
                                                   aggregate_overflow_row, final);
                        }

                        if (query.group_by_with_rollup)
                            executeRollupOrCube(pipeline, Modificator::ROLLUP);
                        else if (query.group_by_with_cube)
                            executeRollupOrCube(pipeline, Modificator::CUBE);

                        if ((query.group_by_with_rollup || query.group_by_with_cube) && expressions.has_having) {
                            if (query.group_by_with_totals)
                                throw Exception(
                                        "WITH TOTALS and WITH ROLLUP or CUBE are not supported together in presence of HAVING",
                                        ErrorCodes::NOT_IMPLEMENTED);
                            executeHaving(pipeline, expressions.before_having);
                        }
                    } else if (expressions.has_having)
                        executeHaving(pipeline, expressions.before_having);

                    executeExpression(pipeline, expressions.before_order_and_select);
                    executeDistinct(pipeline, true, expressions.selected_columns);

                    need_second_distinct_pass = query.distinct && pipeline.hasMixedStreams();
                } else {
                    //不包含聚合操作
                    need_second_distinct_pass = query.distinct && pipeline.hasMixedStreams();

                    if (query.group_by_with_totals && !aggregate_final) {
                        bool final = !query.group_by_with_rollup && !query.group_by_with_cube;
                        executeTotalsAndHaving(pipeline, expressions.has_having, expressions.before_having,
                                               aggregate_overflow_row, final);
                    }

                    if ((query.group_by_with_rollup || query.group_by_with_cube) && !aggregate_final) {
                        if (query.group_by_with_rollup)
                            executeRollupOrCube(pipeline, Modificator::ROLLUP);
                        else if (query.group_by_with_cube)
                            executeRollupOrCube(pipeline, Modificator::CUBE);

                        if (expressions.has_having) {
                            if (query.group_by_with_totals)
                                throw Exception(
                                        "WITH TOTALS and WITH ROLLUP or CUBE are not supported together in presence of HAVING",
                                        ErrorCodes::NOT_IMPLEMENTED);
                            executeHaving(pipeline, expressions.before_having);
                        }
                    }
                }

                //处理order by 操作
                if (expressions.has_order_by) {
                    /** If there is an ORDER BY for distributed query processing,
                      *  but there is no aggregation, then on the remote servers ORDER BY was made
                      *  - therefore, we merge the sorted streams from remote servers.
                      */
                    if (!expressions.first_stage && !expressions.need_aggregate &&
                        !(query.group_by_with_totals && !aggregate_final))
                        executeMergeSorted(pipeline);
                    else    /// Otherwise, just sort.
                        executeOrder(pipeline);
                }

                /** Optimization - if there are several sources and there is LIMIT, then first apply the preliminary LIMIT,
                  * limiting the number of rows in each up to `offset + limit`.
                  */
                if (query.limitLength() && pipeline.hasMoreThanOneStream() && !query.distinct &&
                    !expressions.has_limit_by && !settings.extremes) {
                    executePreLimit(pipeline);
                }

                if (need_second_distinct_pass
                    || query.limitLength()
                    || query.limitBy()
                    || pipeline.stream_with_non_joined_data) {
                    need_merge_streams = true;
                }

                if (need_merge_streams)
                    executeUnion(pipeline);

                /** If there was more than one stream,
                  * then DISTINCT needs to be performed once again after merging all streams.
                  */
                if (need_second_distinct_pass)
                    executeDistinct(pipeline, false, expressions.selected_columns);

                if (expressions.has_limit_by) {
                    executeExpression(pipeline, expressions.before_limit_by);
                    executeLimitBy(pipeline);
                }

                /** We must do projection after DISTINCT because projection may remove some columns.
                  */
                //处理投影操作
                executeProjection(pipeline, expressions.final_projection);

                /** Extremes are calculated before LIMIT, but after LIMIT BY. This is Ok.
                  */
                //处理极值操作
                //极值操作在LIMIT操作之前, 但在LIMIT BY操作之后进行
                executeExtremes(pipeline);

                //处理LIMIT操作
                executeLimit(pipeline);
            }
        }

        //包含全局的子查询  且 子查询集合不为空？？？(应该是SQL包含GLOBAL关键字的时候会用到)
        if (query_analyzer->hasGlobalSubqueries() && !expressions.subqueries_for_sets.empty())
            executeSubqueriesInSetsAndJoins(pipeline, expressions.subqueries_for_sets);
    }


    static UInt64 getLimitUIntValue(const ASTPtr &node, const Context &context) {
        const auto &[field, type] = evaluateConstantExpression(node, context);

        if (!isNativeNumber(type))
            throw Exception("Illegal type " + type->getName() + " of LIMIT expression, must be numeric type",
                            ErrorCodes::INVALID_LIMIT_EXPRESSION);

        Field converted = convertFieldToType(field, DataTypeUInt64());
        if (converted.isNull())
            throw Exception("The value " + applyVisitor(FieldVisitorToString(), field) +
                            " of LIMIT expression is not representable as UInt64",
                            ErrorCodes::INVALID_LIMIT_EXPRESSION);

        return converted.safeGet<UInt64>();
    }

    static std::pair<UInt64, UInt64> getLimitLengthAndOffset(const ASTSelectQuery &query, const Context &context) {
        UInt64 length = 0;
        UInt64 offset = 0;

        if (query.limitLength()) {
            length = getLimitUIntValue(query.limitLength(), context);
            if (query.limitOffset())
                offset = getLimitUIntValue(query.limitOffset(), context);
        }

        return {length, offset};
    }

    static UInt64 getLimitForSorting(const ASTSelectQuery &query, const Context &context) {
        /// Partial sort can be done if there is LIMIT but no DISTINCT or LIMIT BY.
        if (!query.distinct && !query.limitBy()) {
            auto[limit_length, limit_offset] = getLimitLengthAndOffset(query, context);
            return limit_length + limit_offset;
        }
        return 0;
    }


    //重点
    void InterpreterSelectQuery::executeFetchColumns(
            QueryProcessingStage::Enum processing_stage, Pipeline &pipeline,
            const PrewhereInfoPtr &prewhere_info, const Names &columns_to_remove_after_prewhere) {
        auto &query = getSelectQuery();
        const Settings &settings = context.getSettingsRef();

        /// Actions to calculate ALIAS if required. 需要时计算别名的操作
        ExpressionActionsPtr alias_actions;

        if (storage) {//表示需要从表中读取数据
            /// Append columns from the table filter to required
            if (context.hasUserProperty(storage->getDatabaseName(), storage->getTableName(), "filter")) {
                auto initial_required_columns = required_columns;
                ExpressionActionsPtr actions;
                generateFilterActions(actions, storage, context, initial_required_columns);
                auto required_columns_from_filter = actions->getRequiredColumns();

                for (const auto &column : required_columns_from_filter) {
                    if (required_columns.end() == std::find(required_columns.begin(), required_columns.end(), column))
                        required_columns.push_back(column);
                }
            }

            /// Detect, if ALIAS columns are required for query execution   检测是否需要别名列
            auto alias_columns_required = false;
            const ColumnsDescription &storage_columns = storage->getColumns();
            for (const auto &column_name : required_columns) {
                auto column_default = storage_columns.getDefault(column_name);
                if (column_default && column_default->kind == ColumnDefaultKind::Alias) {
                    alias_columns_required = true;
                    break;
                }
            }

            /// There are multiple sources of required columns:
            ///  - raw required columns,
            ///  - columns deduced from ALIAS columns,
            ///  - raw required columns from PREWHERE,
            ///  - columns deduced from ALIAS columns from PREWHERE.

            /// PREWHERE is a special case, since we need to resolve it and pass directly to `IStorage::read()` before any other executions.
            //  PREWHERE是一种特殊情况, 因为我们需要在执行其他任何执行之前处理它并直接传递给IStorage::read()
            if (alias_columns_required) {
                NameSet required_columns_from_prewhere; /// Set of all (including ALIAS) required columns for PREWHERE
                NameSet required_aliases_from_prewhere; /// Set of ALIAS required columns for PREWHERE

                if (prewhere_info) {
                    /// Get some columns directly from PREWHERE expression actions
                    auto prewhere_required_columns = prewhere_info->prewhere_actions->getRequiredColumns();
                    required_columns_from_prewhere.insert(prewhere_required_columns.begin(),
                                                          prewhere_required_columns.end());
                }

                /// Expression, that contains all raw required columns
                ASTPtr required_columns_all_expr = std::make_shared<ASTExpressionList>();

                /// Expression, that contains raw required columns for PREWHERE
                ASTPtr required_columns_from_prewhere_expr = std::make_shared<ASTExpressionList>();

                /// Sort out already known required columns between expressions,
                /// also populate `required_aliases_from_prewhere`.
                for (const auto &column : required_columns) {
                    ASTPtr column_expr;
                    const auto column_default = storage_columns.getDefault(column);
                    bool is_alias = column_default && column_default->kind == ColumnDefaultKind::Alias;
                    if (is_alias)
                        column_expr = setAlias(column_default->expression->clone(), column);
                    else
                        column_expr = std::make_shared<ASTIdentifier>(column);

                    if (required_columns_from_prewhere.count(column)) {
                        required_columns_from_prewhere_expr->children.emplace_back(std::move(column_expr));

                        if (is_alias)
                            required_aliases_from_prewhere.insert(column);
                    } else
                        required_columns_all_expr->children.emplace_back(std::move(column_expr));
                }

                /// Columns, which we will get after prewhere and filter executions.
                NamesAndTypesList required_columns_after_prewhere;
                NameSet required_columns_after_prewhere_set;

                /// Collect required columns from prewhere expression actions.
                if (prewhere_info) {
                    NameSet columns_to_remove(columns_to_remove_after_prewhere.begin(),
                                              columns_to_remove_after_prewhere.end());
                    Block prewhere_actions_result = prewhere_info->prewhere_actions->getSampleBlock();

                    /// Populate required columns with the columns, added by PREWHERE actions and not removed afterwards.
                    /// XXX: looks hacky that we already know which columns after PREWHERE we won't need for sure.
                    for (const auto &column : prewhere_actions_result) {
                        if (prewhere_info->remove_prewhere_column && column.name == prewhere_info->prewhere_column_name)
                            continue;

                        if (columns_to_remove.count(column.name))
                            continue;

                        required_columns_all_expr->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
                        required_columns_after_prewhere.emplace_back(column.name, column.type);
                    }

                    required_columns_after_prewhere_set
                            = ext::map<NameSet>(required_columns_after_prewhere,
                                                [](const auto &it) { return it.name; });
                }

                auto syntax_result = SyntaxAnalyzer(context).analyze(required_columns_all_expr,
                                                                     required_columns_after_prewhere, {}, storage);
                alias_actions = ExpressionAnalyzer(required_columns_all_expr, syntax_result, context).getActions(true);

                /// The set of required columns could be added as a result of adding an action to calculate ALIAS.
                required_columns = alias_actions->getRequiredColumns();

                /// Do not remove prewhere filter if it is a column which is used as alias.
                if (prewhere_info && prewhere_info->remove_prewhere_column)
                    if (required_columns.end()
                        != std::find(required_columns.begin(), required_columns.end(),
                                     prewhere_info->prewhere_column_name))
                        prewhere_info->remove_prewhere_column = false;

                /// Remove columns which will be added by prewhere.
                required_columns.erase(
                        std::remove_if(required_columns.begin(), required_columns.end(), [&](const String &name) {
                            return !!required_columns_after_prewhere_set.count(name);
                        }), required_columns.end());

                if (prewhere_info) {
                    /// Don't remove columns which are needed to be aliased.
                    auto new_actions = std::make_shared<ExpressionActions>(
                            prewhere_info->prewhere_actions->getRequiredColumnsWithTypes(), context);
                    for (const auto &action : prewhere_info->prewhere_actions->getActions()) {
                        if (action.type != ExpressionAction::REMOVE_COLUMN
                            || required_columns.end() ==
                               std::find(required_columns.begin(), required_columns.end(), action.source_name))
                            new_actions->add(action);
                    }
                    prewhere_info->prewhere_actions = std::move(new_actions);

                    auto analyzed_result
                            = SyntaxAnalyzer(context).analyze(required_columns_from_prewhere_expr,
                                                              storage->getColumns().getAllPhysical());
                    prewhere_info->alias_actions
                            = ExpressionAnalyzer(required_columns_from_prewhere_expr, analyzed_result,
                                                 context).getActions(true, false);

                    /// Add (physical?) columns required by alias actions.
                    auto required_columns_from_alias = prewhere_info->alias_actions->getRequiredColumns();
                    Block prewhere_actions_result = prewhere_info->prewhere_actions->getSampleBlock();
                    for (auto &column : required_columns_from_alias)
                        if (!prewhere_actions_result.has(column))
                            if (required_columns.end() ==
                                std::find(required_columns.begin(), required_columns.end(), column))
                                required_columns.push_back(column);

                    /// Add physical columns required by prewhere actions.
                    for (const auto &column : required_columns_from_prewhere)
                        if (required_aliases_from_prewhere.count(column) == 0)
                            if (required_columns.end() ==
                                std::find(required_columns.begin(), required_columns.end(), column))
                                required_columns.push_back(column);
                }
            }
        }

        /// Limitation on the number of columns to read.
        /// It's not applied in 'only_analyze' mode, because the query could be analyzed without removal of unnecessary columns.
        if (!options.only_analyze && settings.max_columns_to_read &&
            required_columns.size() > settings.max_columns_to_read)
            throw Exception("Limit for number of columns to read exceeded. "
                            "Requested: " + toString(required_columns.size())
                            + ", maximum: " + settings.max_columns_to_read.toString(),
                            ErrorCodes::TOO_MANY_COLUMNS);

        /** With distributed query processing, almost no computations are done in the threads, but wait and receive data from remote servers.
         *  If we have 20 remote servers, and max_threads = 8, then it would not be very good connect and ask only 8 servers at a time.
         *  To simultaneously query more remote servers, instead of max_threads, max_distributed_connections is used.
         */
        //在分布式查询处理中, 线程几乎不进行任何计算, 而是等待并从远程服务器接收数据.
        //如果我们有20个远程服务器, 并且max_threads=8, 那么连接就不是很好, 因为一次只能请求8个服务器.
        //要同时查询更多远程服务器, 请使用max_distributed_connections, 而不是max_threads.

        // 如果是分布式查询, 将最大分布式连接max_distributed_connections的值1024设置为max_streams
        bool is_remote = false;
        if (storage && storage->isRemote()) {
            is_remote = true;
            max_streams = settings.max_distributed_connections;
        }

        UInt64 max_block_size = settings.max_block_size;//每个block的大小max_block_size = 65536行. Which blocks by default read the data (by number of rows)

        auto[limit_length, limit_offset] = getLimitLengthAndOffset(query, context);

        /** Optimization - if not specified DISTINCT, WHERE, GROUP, HAVING, ORDER, LIMIT BY but LIMIT is specified, and limit + offset < max_block_size,
         *  then as the block size we will use limit + offset (not to read more from the table than requested),
         *  and also set the number of threads to 1.
         *  优化点: 如果没有指定DISTINCT, WHERE, GROUP, HAVING, ORDER, LIMIT BY, 但是指定了LIMIT, 并且limit + offset < max_block_size
         *         则将block大小设置成limit + offset, 并将线程数设置为1
         */
        if (!query.distinct
            && !query.prewhere()
            && !query.where()
            && !query.groupBy()
            && !query.having()
            && !query.orderBy()
            && !query.limitBy()
            && query.limitLength()
            && !query_analyzer->hasAggregation()
            && limit_length + limit_offset < max_block_size) {
            max_block_size = std::max(UInt64(1), limit_length + limit_offset);
            max_streams = 1;
        }

        if (!max_block_size)
            throw Exception("Setting 'max_block_size' cannot be zero", ErrorCodes::PARAMETER_OUT_OF_BOUND);

        /// Initialize the initial data streams to which the query transforms are superimposed. Table or subquery or prepared input?
        // 将 最原始的数据流 初始化为 包含多个查询转化的流
        if (!pipeline.streams.empty()) {
            /// Prepared input.
            // 说明BlockInputStreamPtr input指针不为空, 只需要从准备好的输入中读取就行了.
            // 不必要再单独生成执行计划了
        } else if (interpreter_subquery) {
            /// Subquery.    从子查询中读取数据
            /// If we need less number of columns that subquery have - update the interpreter.
            if (required_columns.size() < source_header.columns()) {
                ASTPtr subquery = extractTableExpression(query, 0);
                if (!subquery)
                    throw Exception("Subquery expected", ErrorCodes::LOGICAL_ERROR);

                interpreter_subquery = std::make_unique<InterpreterSelectWithUnionQuery>(
                        subquery, getSubqueryContext(context),
                        options.copy().subquery().noModify(), required_columns);

                if (query_analyzer->hasAggregation())
                    interpreter_subquery->ignoreWithTotals();
            }

            pipeline.streams = interpreter_subquery->executeWithMultipleStreams();
        } else if (storage) {
            /// Table.      从表中读取数据

            if (max_streams == 0)
                throw Exception("Logical error: zero number of streams requested", ErrorCodes::LOGICAL_ERROR);

            /// If necessary, we request more sources than the number of threads - to distribute the work evenly over the threads.
            if (max_streams > 1 && !is_remote)
                max_streams *= settings.max_streams_to_max_threads_ratio;

            SelectQueryInfo query_info;
            query_info.query = query_ptr;
            query_info.syntax_analyzer_result = syntax_analyzer_result;
            query_info.sets = query_analyzer->getPreparedSets();
            query_info.prewhere_info = prewhere_info;

            // 重点方法, 仔细看下
            pipeline.streams = storage->read(required_columns, query_info, context, processing_stage, max_block_size,
                                             max_streams);//max_block_size = 65536 行, max_streams=1

            if (pipeline.streams.empty()) {
                //如果streams为空, 则构造NullBlockInputStream并返回(为NULL的流)
                pipeline.streams = {
                        std::make_shared<NullBlockInputStream>(storage->getSampleBlockForColumns(required_columns))};

                if (query_info.prewhere_info)
                    pipeline.transform([&](auto &stream) {
                        stream = std::make_shared<FilterBlockInputStream>(
                                stream,
                                prewhere_info->prewhere_actions,
                                prewhere_info->prewhere_column_name,
                                prewhere_info->remove_prewhere_column);
                    });
            }

            //给表加锁, 保证读取数据的过程中表是线程安全的
            pipeline.transform([&](auto &stream) {
                stream->addTableLock(table_lock);
            });

            /// Set the limits and quota for reading data, the speed and time of the query.
            //设置读取数据的限制和配额、查询的速度和时间
            {
                IBlockInputStream::LocalLimits limits;
                limits.mode = IBlockInputStream::LIMITS_TOTAL;
                limits.size_limits = SizeLimits(settings.max_rows_to_read, settings.max_bytes_to_read,
                                                settings.read_overflow_mode);
                limits.max_execution_time = settings.max_execution_time;
                limits.timeout_overflow_mode = settings.timeout_overflow_mode;

                /** Quota and minimal speed restrictions are checked on the initiating server of the request, and not on remote servers,
                  *  because the initiating server has a summary of the execution of the request on all servers.
                  *
                  * But limits on data size to read and maximum execution time are reasonable to check both on initiator and
                  *  additionally on each remote server, because these limits are checked per block of data processed,
                  *  and remote servers may process way more blocks of data than are received by initiator.
                  */
                if (options.to_stage == QueryProcessingStage::Complete) {
                    limits.min_execution_speed = settings.min_execution_speed;
                    limits.max_execution_speed = settings.max_execution_speed;
                    limits.min_execution_speed_bytes = settings.min_execution_speed_bytes;
                    limits.max_execution_speed_bytes = settings.max_execution_speed_bytes;
                    limits.timeout_before_checking_execution_speed = settings.timeout_before_checking_execution_speed;
                }

                QuotaForIntervals &quota = context.getQuota();

                pipeline.transform([&](auto &stream) {
                    stream->setLimits(limits);

                    if (options.to_stage == QueryProcessingStage::Complete)
                        stream->setQuota(quota);
                });
            }
        } else
            throw Exception("Logical error in InterpreterSelectQuery: nowhere to read", ErrorCodes::LOGICAL_ERROR);

        /// Aliases in table declaration. 用于给表指定了别名的情况下的处理
        if (processing_stage == QueryProcessingStage::FetchColumns && alias_actions) {
            pipeline.transform([&](auto &stream) {
                stream = std::make_shared<ExpressionBlockInputStream>(stream, alias_actions);
            });
        }
    }


    void InterpreterSelectQuery::executeWhere(Pipeline &pipeline, const ExpressionActionsPtr &expression,
                                              bool remove_fiter) {
        //构建FilterBlockInputStream
        pipeline.transform([&](auto &stream) {
            stream = std::make_shared<FilterBlockInputStream>(stream, expression,
                                                              getSelectQuery().where()->getColumnName(), remove_fiter);
        });
    }


    void InterpreterSelectQuery::executeAggregation(Pipeline &pipeline, const ExpressionActionsPtr &expression,
                                                    bool overflow_row, bool final) {
        pipeline.transform([&](auto &stream) {
            stream = std::make_shared<ExpressionBlockInputStream>(stream, expression);
        });

        Names key_names;
        AggregateDescriptions aggregates;
        query_analyzer->getAggregateInfo(key_names, aggregates);

        Block header = pipeline.firstStream()->getHeader();
        ColumnNumbers keys;
        for (const auto &name : key_names)
            keys.push_back(header.getPositionByName(name));
        for (auto &descr : aggregates)
            if (descr.arguments.empty())
                for (const auto &name : descr.argument_names)
                    descr.arguments.push_back(header.getPositionByName(name));

        const Settings &settings = context.getSettingsRef();

        /** Two-level aggregation is useful in two cases:
          * 1. Parallel aggregation is done, and the results should be merged in parallel.
          * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
          */
        /**以下两种情况需要使用两级聚合(Two-level aggregation):
         * 1、并行聚合完成，结果应并行合并;
         * 2、聚合是通过在磁盘上存储临时数据来完成的, 它们需要以节省内存的方式进行合并
         */
        bool allow_to_use_two_level_group_by =
                pipeline.streams.size() > 1 || settings.max_bytes_before_external_group_by != 0;

        Aggregator::Params params(header, keys, aggregates,
                                  overflow_row, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                                  settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
                                  allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold
                                                                  : SettingUInt64(0),
                                  allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes
                                                                  : SettingUInt64(0),
                                  settings.max_bytes_before_external_group_by,
                                  settings.empty_result_for_aggregation_by_empty_set,
                                  context.getTemporaryPath(), settings.max_threads);

        /// If there are several sources, then we perform parallel aggregation
        //如果有多个源, 则并行地进行聚合
        if (pipeline.streams.size() > 1) {
            pipeline.firstStream() = std::make_shared<ParallelAggregatingBlockInputStream>(
                    pipeline.streams, pipeline.stream_with_non_joined_data, params, final,
                    max_streams,
                    settings.aggregation_memory_efficient_merge_threads
                    ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                    : static_cast<size_t>(settings.max_threads));

            pipeline.stream_with_non_joined_data = nullptr;
            pipeline.streams.resize(1);
        } else {//只有一个源
            BlockInputStreams inputs;
            if (!pipeline.streams.empty())
                inputs.push_back(pipeline.firstStream());
            else
                pipeline.streams.resize(1);

            if (pipeline.stream_with_non_joined_data)
                inputs.push_back(pipeline.stream_with_non_joined_data);

            pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(
                    std::make_shared<ConcatBlockInputStream>(inputs), params, final);

            pipeline.stream_with_non_joined_data = nullptr;
        }
    }


    void InterpreterSelectQuery::executeMergeAggregated(Pipeline &pipeline, bool overflow_row, bool final) {
        Names key_names;
        AggregateDescriptions aggregates;
        query_analyzer->getAggregateInfo(key_names, aggregates);

        Block header = pipeline.firstStream()->getHeader();

        ColumnNumbers keys;
        for (const auto &name : key_names)
            keys.push_back(header.getPositionByName(name));

        /** There are two modes of distributed aggregation.
          *
          * 1. In different threads read from the remote servers blocks.
          * Save all the blocks in the RAM. Merge blocks.
          * If the aggregation is two-level - parallelize to the number of buckets.
          *
          * 2. In one thread, read blocks from different servers in order.
          * RAM stores only one block from each server.
          * If the aggregation is a two-level aggregation, we consistently merge the blocks of each next level.
          *
          * The second option consumes less memory (up to 256 times less)
          *  in the case of two-level aggregation, which is used for large results after GROUP BY,
          *  but it can work more slowly.
          */

        const Settings &settings = context.getSettingsRef();

        Aggregator::Params params(header, keys, aggregates, overflow_row, settings.max_threads);

        if (!settings.distributed_aggregation_memory_efficient) {
            /// We union several sources into one, parallelizing the work.
            executeUnion(pipeline);

            /// Now merge the aggregated blocks
            pipeline.firstStream() = std::make_shared<MergingAggregatedBlockInputStream>(pipeline.firstStream(), params,
                                                                                         final, settings.max_threads);
        } else {
            pipeline.firstStream() = std::make_shared<MergingAggregatedMemoryEfficientBlockInputStream>(
                    pipeline.streams, params, final,
                    max_streams,
                    settings.aggregation_memory_efficient_merge_threads
                    ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                    : static_cast<size_t>(settings.max_threads));

            pipeline.streams.resize(1);
        }
    }


    void InterpreterSelectQuery::executeHaving(Pipeline &pipeline, const ExpressionActionsPtr &expression) {
        pipeline.transform([&](auto &stream) {
            stream = std::make_shared<FilterBlockInputStream>(stream, expression,
                                                              getSelectQuery().having()->getColumnName());
        });
    }


    void InterpreterSelectQuery::executeTotalsAndHaving(Pipeline &pipeline, bool has_having,
                                                        const ExpressionActionsPtr &expression, bool overflow_row,
                                                        bool final) {
        executeUnion(pipeline);

        const Settings &settings = context.getSettingsRef();

        pipeline.firstStream() = std::make_shared<TotalsHavingBlockInputStream>(
                pipeline.firstStream(),
                overflow_row,
                expression,
                has_having ? getSelectQuery().having()->getColumnName() : "",
                settings.totals_mode,
                settings.totals_auto_threshold,
                final);
    }

    void InterpreterSelectQuery::executeRollupOrCube(Pipeline &pipeline, Modificator modificator) {
        executeUnion(pipeline);

        Names key_names;
        AggregateDescriptions aggregates;
        query_analyzer->getAggregateInfo(key_names, aggregates);

        Block header = pipeline.firstStream()->getHeader();

        ColumnNumbers keys;

        for (const auto &name : key_names)
            keys.push_back(header.getPositionByName(name));

        const Settings &settings = context.getSettingsRef();

        Aggregator::Params params(header, keys, aggregates,
                                  false, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                                  settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
                                  SettingUInt64(0), SettingUInt64(0),
                                  settings.max_bytes_before_external_group_by,
                                  settings.empty_result_for_aggregation_by_empty_set,
                                  context.getTemporaryPath(), settings.max_threads);

        if (modificator == Modificator::ROLLUP)
            pipeline.firstStream() = std::make_shared<RollupBlockInputStream>(pipeline.firstStream(), params);
        else
            pipeline.firstStream() = std::make_shared<CubeBlockInputStream>(pipeline.firstStream(), params);
    }


    void InterpreterSelectQuery::executeExpression(Pipeline &pipeline, const ExpressionActionsPtr &expression) {
        pipeline.transform([&](auto &stream) {
            stream = std::make_shared<ExpressionBlockInputStream>(stream, expression);
        });
    }


    static SortDescription getSortDescription(const ASTSelectQuery &query) {
        SortDescription order_descr;
        order_descr.reserve(query.orderBy()->children.size());
        for (const auto &elem : query.orderBy()->children) {
            String name = elem->children.front()->getColumnName();
            const auto &order_by_elem = elem->as<ASTOrderByElement &>();

            std::shared_ptr<Collator> collator;
            if (order_by_elem.collation)
                collator = std::make_shared<Collator>(order_by_elem.collation->as<ASTLiteral &>().value.get<String>());

            order_descr.emplace_back(name, order_by_elem.direction, order_by_elem.nulls_direction, collator);
        }

        return order_descr;
    }


    void InterpreterSelectQuery::executeOrder(Pipeline &pipeline) {
        auto &query = getSelectQuery();
        SortDescription order_descr = getSortDescription(query);
        UInt64 limit = getLimitForSorting(query, context);

        const Settings &settings = context.getSettingsRef();

        pipeline.transform([&](auto &stream) {
            auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, limit);

            /// Limits on sorting
            IBlockInputStream::LocalLimits limits;
            limits.mode = IBlockInputStream::LIMITS_TOTAL;
            limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort,
                                            settings.sort_overflow_mode);
            sorting_stream->setLimits(limits);

            stream = sorting_stream;
        });

        /// If there are several streams, we merge them into one
        executeUnion(pipeline);

        /// Merge the sorted blocks.
        pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
                pipeline.firstStream(), order_descr, settings.max_block_size, limit,
                settings.max_bytes_before_remerge_sort,
                settings.max_bytes_before_external_sort, context.getTemporaryPath());
    }


    void InterpreterSelectQuery::executeMergeSorted(Pipeline &pipeline) {
        auto &query = getSelectQuery();
        SortDescription order_descr = getSortDescription(query);
        UInt64 limit = getLimitForSorting(query, context);

        const Settings &settings = context.getSettingsRef();

        /// If there are several streams, then we merge them into one
        if (pipeline.hasMoreThanOneStream()) {
            unifyStreams(pipeline);

            /** MergingSortedBlockInputStream reads the sources sequentially.
              * To make the data on the remote servers prepared in parallel, we wrap it in AsynchronousBlockInputStream.
              */
            pipeline.transform([&](auto &stream) {
                stream = std::make_shared<AsynchronousBlockInputStream>(stream);
            });

            /// Merge the sorted sources into one sorted source.
            pipeline.firstStream() = std::make_shared<MergingSortedBlockInputStream>(pipeline.streams, order_descr,
                                                                                     settings.max_block_size, limit);
            pipeline.streams.resize(1);
        }
    }


    void InterpreterSelectQuery::executeProjection(Pipeline &pipeline, const ExpressionActionsPtr &expression) {
        pipeline.transform([&](auto &stream) {
            stream = std::make_shared<ExpressionBlockInputStream>(stream, expression);
        });
    }


    void InterpreterSelectQuery::executeDistinct(Pipeline &pipeline, bool before_order, Names columns) {
        auto &query = getSelectQuery();
        if (query.distinct) {//如果不包含DISTINCT关键字, 直接返回即可
            const Settings &settings = context.getSettingsRef();

            auto[limit_length, limit_offset] = getLimitLengthAndOffset(query, context);
            UInt64 limit_for_distinct = 0;

            /// If after this stage of DISTINCT ORDER BY is not executed, then you can get no more than limit_length + limit_offset of different rows.
            if (!query.orderBy() || !before_order)
                limit_for_distinct = limit_length + limit_offset;

            pipeline.transform([&](auto &stream) {
                SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct,
                                  settings.distinct_overflow_mode);
                stream = std::make_shared<DistinctBlockInputStream>(stream, limits, limit_for_distinct, columns);
            });
        }
    }


    void InterpreterSelectQuery::executeUnion(Pipeline &pipeline) {
        /// If there are still several streams, then we combine them into one
        if (pipeline.hasMoreThanOneStream()) {
            unifyStreams(pipeline);

            pipeline.firstStream() = std::make_shared<UnionBlockInputStream>(pipeline.streams,
                                                                             pipeline.stream_with_non_joined_data,
                                                                             max_streams);
            pipeline.stream_with_non_joined_data = nullptr;
            pipeline.streams.resize(1);
            pipeline.union_stream = true;
        } else if (pipeline.stream_with_non_joined_data) {
            pipeline.streams.push_back(pipeline.stream_with_non_joined_data);
            pipeline.stream_with_non_joined_data = nullptr;
        }
    }


/// Preliminary LIMIT - is used in every source, if there are several sources, before they are combined.
    void InterpreterSelectQuery::executePreLimit(Pipeline &pipeline) {
        auto &query = getSelectQuery();
        /// If there is LIMIT
        if (query.limitLength()) {
            auto[limit_length, limit_offset] = getLimitLengthAndOffset(query, context);
            pipeline.transform([&, limit = limit_length + limit_offset](auto &stream) {
                stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, false);
            });
        }
    }


    void InterpreterSelectQuery::executeLimitBy(Pipeline &pipeline) {
        auto &query = getSelectQuery();
        if (!query.limitByLength() || !query.limitBy())
            return;

        Names columns;
        for (const auto &elem : query.limitBy()->children)
            columns.emplace_back(elem->getColumnName());
        UInt64 length = getLimitUIntValue(query.limitByLength(), context);
        UInt64 offset = (query.limitByOffset() ? getLimitUIntValue(query.limitByOffset(), context) : 0);

        pipeline.transform([&](auto &stream) {
            stream = std::make_shared<LimitByBlockInputStream>(stream, length, offset, columns);
        });
    }


// TODO: move to anonymous namespace
    bool hasWithTotalsInAnySubqueryInFromClause(const ASTSelectQuery &query) {
        if (query.group_by_with_totals)
            return true;

        /** NOTE You can also check that the table in the subquery is distributed, and that it only looks at one shard.
          * In other cases, totals will be computed on the initiating server of the query, and it is not necessary to read the data to the end.
          */

        if (auto query_table = extractTableExpression(query, 0)) {
            if (const auto *ast_union = query_table->as<ASTSelectWithUnionQuery>()) {
                for (const auto &elem : ast_union->list_of_selects->children)
                    if (hasWithTotalsInAnySubqueryInFromClause(elem->as<ASTSelectQuery &>()))
                        return true;
            }
        }

        return false;
    }


    void InterpreterSelectQuery::executeLimit(Pipeline &pipeline) {
        auto &query = getSelectQuery();
        /// If there is LIMIT
        // 如果SQL中包含了LIMIT关键字, query.limitLength() = true
        if (query.limitLength()) {
            /** Rare case:
              *  if there is no WITH TOTALS and there is a subquery in FROM, and there is WITH TOTALS on one of the levels,
              *  then when using LIMIT, you should read the data to the end, rather than cancel the query earlier,
              *  because if you cancel the query, we will not get `totals` data from the remote server.
              *
              * Another case:
              *  if there is WITH TOTALS and there is no ORDER BY, then read the data to the end,
              *  otherwise TOTALS is counted according to incomplete data.
              */
            bool always_read_till_end = false;

            if (query.group_by_with_totals && !query.orderBy())
                always_read_till_end = true;

            if (!query.group_by_with_totals && hasWithTotalsInAnySubqueryInFromClause(query))
                always_read_till_end = true;

            UInt64 limit_length;
            UInt64 limit_offset;
            std::tie(limit_length, limit_offset) = getLimitLengthAndOffset(query, context);

            //构建LimitBlockInputStream
            pipeline.transform([&](auto &stream) {
                stream = std::make_shared<LimitBlockInputStream>(stream, limit_length, limit_offset,
                                                                 always_read_till_end);
            });
        }
    }


    void InterpreterSelectQuery::executeExtremes(Pipeline &pipeline) {
        if (!context.getSettingsRef().extremes)
            return;

        pipeline.transform([&](auto &stream) {
            stream->enableExtremes();
        });
    }


    void InterpreterSelectQuery::executeSubqueriesInSetsAndJoins(Pipeline &pipeline, SubqueriesForSets &subqueries_for_sets) {
        //执行UNION, 将多个流UNION成一个流(构造一个上层父类流, 父类流包含了这多个子类流)
        executeUnion(pipeline);
        //CreatingSetsBlockInputStream这个流会在readPrefix()方法中或在读取第一个块之前, 初始化所有传递的sets
        pipeline.firstStream() = std::make_shared<CreatingSetsBlockInputStream>(pipeline.firstStream(), subqueries_for_sets, context);
    }

    void InterpreterSelectQuery::unifyStreams(Pipeline &pipeline) {
        if (pipeline.hasMoreThanOneStream()) {
            /// Unify streams in case they have different headers.
            auto first_header = pipeline.streams.at(0)->getHeader();
            for (size_t i = 1; i < pipeline.streams.size(); ++i) {
                auto &stream = pipeline.streams[i];
                auto header = stream->getHeader();
                auto mode = ConvertingBlockInputStream::MatchColumnsMode::Name;
                if (!blocksHaveEqualStructure(first_header, header))
                    stream = std::make_shared<ConvertingBlockInputStream>(context, stream, first_header, mode);
            }
        }
    }


    void InterpreterSelectQuery::ignoreWithTotals() {
        getSelectQuery().group_by_with_totals = false;
    }


    void InterpreterSelectQuery::initSettings() {
        auto &query = getSelectQuery();
        if (query.settings())
            InterpreterSetQuery(query.settings(), context).executeForCurrentContext();
    }

}
