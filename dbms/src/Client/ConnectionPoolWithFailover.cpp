#include <Client/ConnectionPoolWithFailover.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/DNS.h>

#include <Common/getFQDNOrHostName.h>
#include <Common/isLocalAddress.h>
#include <Common/ProfileEvents.h>
#include <Core/Settings.h>


namespace ProfileEvents {
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
}

namespace DB {

    namespace ErrorCodes {
        extern const int NETWORK_ERROR;
        extern const int SOCKET_TIMEOUT;
        extern const int LOGICAL_ERROR;
    }


    ConnectionPoolWithFailover::ConnectionPoolWithFailover(
            ConnectionPoolPtrs nested_pools_,
            LoadBalancing load_balancing,
            size_t max_tries_,
            time_t decrease_error_period_)
            : Base(std::move(nested_pools_), max_tries_, decrease_error_period_,
                   &Logger::get("ConnectionPoolWithFailover")), default_load_balancing(load_balancing) {
        const std::string &local_hostname = getFQDNOrHostName();

        hostname_differences.resize(nested_pools.size());
        for (size_t i = 0; i < nested_pools.size(); ++i) {
            ConnectionPool &connection_pool = dynamic_cast<ConnectionPool &>(*nested_pools[i]);
            hostname_differences[i] = getHostNameDifference(local_hostname, connection_pool.getHost());
        }
    }

    IConnectionPool::Entry ConnectionPoolWithFailover::get(const Settings *settings, bool /*force_connected*/) {
        TryGetEntryFunc try_get_entry = [&](NestedPool &pool, std::string &fail_message) {
            return tryGetEntry(pool, fail_message, settings);
        };

        GetPriorityFunc get_priority;
        switch (settings ? LoadBalancing(settings->load_balancing) : default_load_balancing) {
            case LoadBalancing::NEAREST_HOSTNAME:
                get_priority = [&](size_t i) { return hostname_differences[i]; };
                break;
            case LoadBalancing::IN_ORDER:
                get_priority = [](size_t i) { return i; };
                break;
            case LoadBalancing::RANDOM:
                break;
            case LoadBalancing::FIRST_OR_RANDOM:
                get_priority = [](size_t i) -> size_t { return i >= 1; };
                break;
        }

        return Base::get(try_get_entry, get_priority);
    }

    std::vector<IConnectionPool::Entry>
    ConnectionPoolWithFailover::getMany(const Settings *settings, PoolMode pool_mode) {
        TryGetEntryFunc try_get_entry = [&](NestedPool &pool, std::string &fail_message) {
            return tryGetEntry(pool, fail_message, settings);
        };

        std::vector<TryResult> results = getManyImpl(settings, pool_mode, try_get_entry);

        std::vector<Entry> entries;
        entries.reserve(results.size());
        for (auto &result : results)
            entries.emplace_back(std::move(result.entry));
        return entries;
    }

    std::vector<ConnectionPoolWithFailover::TryResult>
    ConnectionPoolWithFailover::getManyForTableFunction(const Settings *settings, PoolMode pool_mode) {
        TryGetEntryFunc try_get_entry = [&](NestedPool &pool, std::string &fail_message) {
            return tryGetEntry(pool, fail_message, settings);
        };

        return getManyImpl(settings, pool_mode, try_get_entry);
    }

    std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyChecked(
            const Settings *settings, PoolMode pool_mode, const QualifiedTableName &table_to_check) {
        TryGetEntryFunc try_get_entry = [&](NestedPool &pool, std::string &fail_message) {
            return tryGetEntry(pool, fail_message, settings, &table_to_check);
        };

        return getManyImpl(settings, pool_mode, try_get_entry);
    }

//这里涉及到了负载均衡策略
    std::vector<ConnectionPoolWithFailover::TryResult> ConnectionPoolWithFailover::getManyImpl(
            const Settings *settings,
            PoolMode pool_mode,
            const TryGetEntryFunc &try_get_entry) {
        //skip_unavailable_shards = false, 所以min_entries=1
        size_t min_entries = (settings && settings->skip_unavailable_shards) ? 0 : 1;
        size_t max_entries;
        if (pool_mode == PoolMode::GET_ALL) {
            min_entries = nested_pools.size();
            max_entries = nested_pools.size();
        } else if (pool_mode == PoolMode::GET_ONE)
            max_entries = 1;
        else if (pool_mode == PoolMode::GET_MANY)
            //max_parallel_replicas=1, 执行查询时每个分片所使用的最大副本数. 对于一致性(获取同一分区的不同部分), 此选项仅适用于指定的采样密钥. 副本的延迟不受控制
            //max_parallel_replicas = 1, 所以max_entries=1
            max_entries = settings ? size_t(settings->max_parallel_replicas) : 1;
        else
            throw DB::Exception("Unknown pool allocation mode", DB::ErrorCodes::LOGICAL_ERROR);

        GetPriorityFunc get_priority;
        //load_balancing = RANDOM, 所以这个get_priority对所有的连接池应该都一样
        switch (settings ? LoadBalancing(settings->load_balancing) : default_load_balancing) {
            case LoadBalancing::NEAREST_HOSTNAME:
                get_priority = [&](size_t i) { return hostname_differences[i]; };
                break;
            case LoadBalancing::IN_ORDER:
                get_priority = [](size_t i) { return i; };
                break;
            case LoadBalancing::RANDOM:
                break;
            case LoadBalancing::FIRST_OR_RANDOM:
                get_priority = [](size_t i) -> size_t { return i >= 1; };
                break;
        }

        //fallback_to_stale_replicas_for_distributed_queries=1, 即fallback_to_stale_replicas=true
        bool fallback_to_stale_replicas = settings ? bool(settings->fallback_to_stale_replicas_for_distributed_queries)
                                                   : true;

        //调用getMany()这个方法
        return Base::getMany(min_entries, max_entries, try_get_entry, get_priority, fallback_to_stale_replicas);
    }

    ConnectionPoolWithFailover::TryResult
    ConnectionPoolWithFailover::tryGetEntry(
            IConnectionPool &pool,
            std::string &fail_message,
            const Settings *settings,
            const QualifiedTableName *table_to_check) {
        TryResult result;
        try {
            result.entry = pool.get(settings, /* force_connected = */ false);

            UInt64 server_revision = 0;
            //指定了表名
            if (table_to_check)
                server_revision = result.entry->getServerRevision();

            //没有指定表名 或者 server版本较低
            if (!table_to_check || server_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS) {
                result.entry->forceConnected();
                result.is_usable = true;
                result.is_up_to_date = true;
                return result;
            }

            /// Only status of the remote table corresponding to the Distributed table is taken into account.
            // 只考虑与分布式表相对应的远程物理表的状态
            /// TODO: request status for joined tables also.
            TablesStatusRequest status_request;
            status_request.tables.emplace(*table_to_check);

            //将需要查询的物理表封装成TablesStatusRequest发送给Server端, 获取这个物理表的状态, 保存在TablesStatusResponse中
            //TablesStatusResponse的table_states_by_id这个map中保存了物理表的状态, map的键是库名表名, map的值是表的状态TablesStatus
            TablesStatusResponse status_response = result.entry->getTablesStatus(status_request);
            auto table_status_it = status_response.table_states_by_id.find(*table_to_check);

            if (table_status_it == status_response.table_states_by_id.end()) {//没有这个表
                fail_message = "There is no table " + table_to_check->database + "." + table_to_check->table
                               + " on server: " + result.entry->getDescription();
                LOG_WARNING(log, fail_message);
                ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);

                return result;
            }

            //如果有这个表, 则is_usable = true
            result.is_usable = true;

            //max_replica_delay_for_distributed_queries=300s
            //如果设置了max_replica_delay_for_distributed_queries, 则在复制表的分布式查询中将选择复制延迟小于指定值的服务器(单位: 秒; 不包括等于)
            UInt64 max_allowed_delay = settings ? UInt64(settings->max_replica_delay_for_distributed_queries) : 0;
            //max_allowed_delay=0, 表示不考虑延迟, 则默认表中的数据总是最新的, 设置is_up_to_date = true
            if (!max_allowed_delay) {
                result.is_up_to_date = true;
                return result;
            }

            //max_allowed_delay != 0, 表示需要考虑延迟, 表的绝对延迟小于最大允许延迟时才设置is_up_to_date = true
            UInt32 delay = table_status_it->second.absolute_delay;

            if (delay < max_allowed_delay)
                result.is_up_to_date = true;
            else {                                  //否则设置is_up_to_date = false, 并设置staleness = delay
                result.is_up_to_date = false;
                result.staleness = delay;

                LOG_TRACE(
                        log, "Server " << result.entry->getDescription() << " has unacceptable replica delay "
                                       << "for table " << table_to_check->database << "." << table_to_check->table
                                       << ": " << delay);
                ProfileEvents::increment(ProfileEvents::DistributedConnectionStaleReplica);
            }
        }
        catch (const Exception &e) {
            if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT
                && e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
                throw;

            fail_message = getCurrentExceptionMessage(/* with_stacktrace = */ false);

            if (!result.entry.isNull()) {
                result.entry->disconnect();
                result.reset();
            }
        }
        return result;
    }

}
