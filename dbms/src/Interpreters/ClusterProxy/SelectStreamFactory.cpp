#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/LazyBlockInputStream.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <common/logger_useful.h>


namespace ProfileEvents {
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
}

namespace DB {

    namespace ErrorCodes {
        extern const int ALL_REPLICAS_ARE_STALE;
    }

    namespace ClusterProxy {

        SelectStreamFactory::SelectStreamFactory(
                const Block &header_,
                QueryProcessingStage::Enum processed_stage_,
                QualifiedTableName main_table_,
                const Tables &external_tables_)
                : header(header_),
                  processed_stage{processed_stage_},
                  main_table(std::move(main_table_)),
                  table_func_ptr{nullptr},
                  external_tables{external_tables_} {
        }

        SelectStreamFactory::SelectStreamFactory(
                const Block &header_,
                QueryProcessingStage::Enum processed_stage_,
                ASTPtr table_func_ptr_,
                const Tables &external_tables_)
                : header(header_),
                  processed_stage{processed_stage_},
                  table_func_ptr{table_func_ptr_},
                  external_tables{external_tables_} {
        }

        namespace {

            BlockInputStreamPtr createLocalStream(const ASTPtr &query_ast, const Context &context,
                                                  QueryProcessingStage::Enum processed_stage) {
                InterpreterSelectQuery interpreter{query_ast, context, SelectQueryOptions(processed_stage)};
                BlockInputStreamPtr stream = interpreter.execute().in;

                /** Materialization is needed, since from remote servers the constants come materialized.
                  * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
                  * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
                  */
                // 此处 物化Materialization 的意思是 展开常量列
                //常量从远程服务器发送过来的时候是展开的, 所以本地的常量也需要展开
                return std::make_shared<MaterializingBlockInputStream>(stream);
            }

        }

        //遍历数据的所有的分片，针对每个分片
        void SelectStreamFactory::createForShard(
                const Cluster::ShardInfo &shard_info,
                const String &query, const ASTPtr &query_ast,
                const Context &context, const ThrottlerPtr &throttler,
                BlockInputStreams &res) {
            auto emplace_local_stream = [&]()//将 数据流 放在本地
            {
                res.emplace_back(createLocalStream(query_ast, context, processed_stage));
            };

            auto emplace_remote_stream = [&]()//将 数据流 发送给远程服务器
            {
                //构建RemoteBlockInputStream
                auto stream = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, header, context, nullptr,
                                                                       throttler, external_tables, processed_stage);
                stream->setPoolMode(PoolMode::GET_MANY);
                if (!table_func_ptr)
                    stream->setMainTable(main_table);
                res.emplace_back(std::move(stream));
            };

            const auto &settings = context.getSettingsRef();

            //prefer_localhost_replica = 1 且 本地服务器上存在这个分片(shard_info.isLocal() = true), 就使用本地分片数据.
            //                              如果本地服务器上没有这个分片, 则只能连接远程获取该分片的数据emplace_remote_stream
            //prefer_localhost_replica = 0 则 连接远程获取该分片的数据emplace_remote_stream
            if (settings.prefer_localhost_replica && shard_info.isLocal()) {
                //运行到这里表示本地服务器上存在这个分片
                StoragePtr main_table_storage;//根据库名表名在本地服务器的分片上的找到需要查询的这个表

                if (table_func_ptr) {
                    const auto *table_function = table_func_ptr->as<ASTFunction>();
                    main_table_storage = TableFunctionFactory::instance().get(table_function->name, context)->execute(
                            table_func_ptr, context);
                } else
                    main_table_storage = context.tryGetTable(main_table.database, main_table.table);

                //本地服务器的这个分片上没有这个表
                if (!main_table_storage) /// Table is absent on a local server.
                {
                    ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);
                    //shard_info.hasRemoteConnections() 本次查询是否是远程发过来的(是否有远程副本), 如果是则需要emplace_remote_stream
                    if (shard_info.hasRemoteConnections()) {
                        LOG_WARNING(
                                &Logger::get("ClusterProxy::SelectStreamFactory"),
                                "There is no table " << main_table.database << "." << main_table.table
                                                     << " on local replica of shard " << shard_info.shard_num
                                                     << ", will try remote replicas.");
                        emplace_remote_stream();
                    } else
                        emplace_local_stream();  /// Let it fail the usual way.

                    return;
                }

                //运行到这里, 表示这个表在本地服务器的这个分片上

                //通过一个动态转换来判断这个表有没有副本
                const auto *replicated_storage = dynamic_cast<const StorageReplicatedMergeTree *>(main_table_storage.get());

                //该分片在远程没有副本, 只能使用本地分片数据
                if (!replicated_storage) {
                    /// Table is not replicated, use local server.
                    emplace_local_stream();
                    return;
                }

                // 代码运行到这里, 说明对于当前分片, 本地服务器上有这个表, 且该分片也有远程副本.
                // 那应该怎么选呢？
                // 到了这一步就需要考虑应该选择本地的还是远程的了

                // 如果设置了max_replica_delay_for_distributed_queries(分布式查询的最大副本延迟)这个参数, 则复制表的分布式查询将选择复制延迟时间(秒)小于指定值(不包括该指定值)的服务器.
                // 零意味着不考虑延迟。
                UInt64 max_allowed_delay = settings.max_replica_delay_for_distributed_queries;

                //没有设置max_allowed_delay这个参数, 则不考虑延迟, 优先使用本地分片数据
                if (!max_allowed_delay) {
                    emplace_local_stream();
                    return;
                }

                //设置了max_allowed_delay这个参数, 就先获取副本的绝对延迟 (这里获取的应该是本地副本的延迟时间)
                UInt32 local_delay = replicated_storage->getAbsoluteDelay();
                //如果本地副本的延迟时间小于max_allowed_delay, 说明本地副本是可以使用的
                if (local_delay < max_allowed_delay) {
                    emplace_local_stream();
                    return;
                }

                /// If we reached this point, local replica is stale.
                // 如果代码执行到这里, 表示本地副本已经过期了(复制延迟时间 >= 指定值300s)
                ProfileEvents::increment(ProfileEvents::DistributedConnectionStaleReplica);
                LOG_WARNING(
                        &Logger::get("ClusterProxy::SelectStreamFactory"),
                        "Local replica of shard " << shard_info.shard_num << " is stale (delay: " << local_delay
                                                  << "s.)");

                // 代码运行到这里表示设置了max_replica_delay_for_distributed_queries(分布式查询的最大副本延迟)这个参数, 并且本地的副本已过期了.

                // 如果设置了fallback_to_stale_replicas_for_distributed_queries=0, 表示不允许使用过期的副本,
                // 则将查看当前分片是不是有远程副本, 如果由则使用远程副本, 如果没有则报错
                if (!settings.fallback_to_stale_replicas_for_distributed_queries) {
                    if (shard_info.hasRemoteConnections()) {
                        /// If we cannot fallback, then we cannot use local replica. Try our luck with remote replicas.
                        emplace_remote_stream();
                        return;
                    } else
                        throw Exception(
                                "Local replica of shard " + toString(shard_info.shard_num)
                                + " is stale (delay: " + toString(local_delay) + "s.), but no other replica configured",
                                ErrorCodes::ALL_REPLICAS_ARE_STALE);
                }

                // 如果设置了fallback_to_stale_replicas_for_distributed_queries=1, 表示允许使用过期的副本,
                // 再判断当前分片是不是有远程副本, 如果没有远程副本. 则只能使用本地过期的副本
                if (!shard_info.hasRemoteConnections()) {
                    /// There are no remote replicas but we are allowed to fall back to stale local replica.
                    emplace_local_stream();
                    return;
                }

                //代码运行到这里表示允许使用过期的副本, 且远程也有副本
                /// Try our luck with remote replicas, but if they are stale too, then fallback to local replica.
                /// Do it lazily to avoid connecting in the main thread.
                //于是就先尝试使用远程副本, 但如果它们也过期了, 则退回使用本地副本
                //懒洋洋地做这件事以避免在主线程中连接(惰性创建连接)

                //惰性创建数据流(类比spark中, 一次行动操作触发一次计算), 这里也是, 先捋清底层数据都有哪些, 再创建stream
                auto lazily_create_stream = [
                        pool = shard_info.pool, shard_num = shard_info.shard_num, query, header = header, query_ast, context, throttler,
                        main_table = main_table, table_func_ptr = table_func_ptr, external_tables = external_tables, stage = processed_stage,
                        local_delay]()
                        -> BlockInputStreamPtr {
                    std::vector<ConnectionPoolWithFailover::TryResult> try_results;
                    try {
                        if (table_func_ptr)
                            try_results = pool->getManyForTableFunction(&context.getSettingsRef(), PoolMode::GET_MANY);
                        else
                            try_results = pool->getManyChecked(&context.getSettingsRef(), PoolMode::GET_MANY,
                                                               main_table);
                    }
                    catch (const Exception &ex) {
                        if (ex.code() == ErrorCodes::ALL_CONNECTION_TRIES_FAILED)
                            LOG_WARNING(
                                    &Logger::get("ClusterProxy::SelectStreamFactory"),
                                    "Connections to remote replicas of local shard " << shard_num
                                                                                     << " failed, will use stale local replica");
                        else
                            throw;
                    }

                    double max_remote_delay = 0.0;
                    for (const auto &try_result : try_results) {
                        if (!try_result.is_up_to_date)
                            max_remote_delay = std::max(try_result.staleness, max_remote_delay);
                    }

                    if (try_results.empty() || local_delay < max_remote_delay)
                        return createLocalStream(query_ast, context, stage);
                    else {
                        std::vector<IConnectionPool::Entry> connections;
                        connections.reserve(try_results.size());
                        for (auto &try_result : try_results)
                            connections.emplace_back(std::move(try_result.entry));

                        return std::make_shared<RemoteBlockInputStream>(
                                std::move(connections), query, header, context, nullptr, throttler, external_tables,
                                stage);
                    }
                };

                res.emplace_back(std::make_shared<LazyBlockInputStream>("LazyShardWithLocalReplica", header,
                                                                        lazily_create_stream));
            } else
                emplace_remote_stream();
        }

    }
}
