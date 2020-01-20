#pragma once

#include <map>
#include <Core/Settings.h>
#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Poco/Net/SocketAddress.h>

namespace DB {

/// Cluster contains connection pools to each node
/// With the local nodes, the connection is not established, but the request is executed directly.
/// Therefore we store only the number of local nodes
/// In the config, the cluster includes nodes <node> or <shard>
//Cluster包含到每个节点的连接池
//对于本地节点, 不会建立连接, 而是直接执行请求
//
//因此我们只存储本地节点的数量
//
//在配置文件中，集群包括的节点由<node>或<shard>指定
    class Cluster {
    public:
        Cluster(const Poco::Util::AbstractConfiguration &config, const Settings &settings, const String &cluster_name);

        /// Construct a cluster by the names of shards and replicas.
        /// Local are treated as well as remote ones if treat_local_as_remote is true.
        /// 'clickhouse_port' - port that this server instance listen for queries.
        /// This parameter is needed only to check that some address is local (points to ourself).
        // 根据分片和副本名构造Cluster.
        // 如果treat_local_as_remote=true, 将本地副本也当作远程副本使用.
        // clickhouse_port是当前服务器实例监听的端口. 这个参数仅用于检查某些地址是否为本地地址(是否指向我们自己)
        Cluster(const Settings &settings, const std::vector<std::vector<String>> &names,
                const String &username, const String &password,
                UInt16 clickhouse_port, bool treat_local_as_remote, bool secure = false);

        Cluster(const Cluster &) = delete;

        Cluster &operator=(const Cluster &) = delete;

        /// is used to set a limit on the size of the timeout
        static Poco::Timespan saturate(const Poco::Timespan &v, const Poco::Timespan &limit);

    public:
        struct Address {
            /** In configuration file,
            * addresses are located either in <node> elements:
            * <node>
            *     <host>example01-01-1</host>
            *     <port>9000</port>
            *     <!-- <user>, <password>, <default_database> if needed -->
            * </node>
            * ...
            * or in <shard> and inside in <replica> elements:
            * <shard>
            *     <replica>
            *         <host>example01-01-1</host>
            *         <port>9000</port>
            *         <!-- <user>, <password>, <default_database>. <secure> if needed -->
            *    </replica>
            * </shard>
            */

            String host_name;
            UInt16 port;
            String user;
            String password;
            /// This database is selected when no database is specified for Distributed table
            String default_database;
            /// The locality is determined at the initialization, and is not changed even if DNS is changed
            bool is_local;//在初始化时确定, 不因DNS改变而改变
            bool user_specified = false;

            Protocol::Compression compression = Protocol::Compression::Enable;
            Protocol::Secure secure = Protocol::Secure::Disable;

            Address() = default;

            Address(const Poco::Util::AbstractConfiguration &config, const String &config_prefix);

            Address(const String &host_port_, const String &user_, const String &password_, UInt16 clickhouse_port,
                    bool secure_ = false);

            /// Returns 'escaped_host_name:port'
            String toString() const;

            /// Returns 'host_name:port'
            String readableString() const;

            static String toString(const String &host_name, UInt16 port);

            static std::pair<String, UInt16> fromString(const String &host_port_string);

            /// Retrurns escaped user:password@resolved_host_address:resolved_host_port#default_database
            String toFullString() const;

            static Address fromFullString(const String &address_full_string);

            /// Returns initially resolved address
            Poco::Net::SocketAddress getResolvedAddress() const {
                return initially_resolved_address;
            }

            auto tuple() const { return std::tie(host_name, port, secure, user, password, default_database); }

            bool operator==(const Address &other) const { return tuple() == other.tuple(); }

        private:
            Poco::Net::SocketAddress initially_resolved_address;
        };

        using Addresses = std::vector<Address>;
        using AddressesWithFailover = std::vector<Addresses>;

        struct ShardInfo {
        public:
            bool isLocal() const { return !local_addresses.empty(); }

            bool hasRemoteConnections() const { return local_addresses.size() != per_replica_pools.size(); }

            size_t getLocalNodeCount() const { return local_addresses.size(); }

            bool hasInternalReplication() const { return has_internal_replication; }

        public:
            /// Name of directory for asynchronous write to StorageDistributed if has_internal_replication
            //有复制表(has_internal_replication=true)时, 分布式表引擎异步写入数据的目录
            std::string dir_name_for_internal_replication;
            /// Number of the shard, the indexation begins with 1
            //第几个分片
            UInt32 shard_num = 0;
            UInt32 weight = 1;
            Addresses local_addresses;
            /// nullptr if there are no remote addresses
            //带容错机制的连接池, 无远程副本时是为nullptr
            ConnectionPoolWithFailoverPtr pool;
            /// Connection pool for each replica, contains nullptr for local replicas
            //针对每个副本的连接池, 本地副本的连接池为nullptr
            ConnectionPoolPtrs per_replica_pools;
            //是否有复制表
            bool has_internal_replication = false;
        };

        using ShardsInfo = std::vector<ShardInfo>;

        String getHashOfAddresses() const { return hash_of_addresses; }

        const ShardsInfo &getShardsInfo() const { return shards_info; }

        const AddressesWithFailover &getShardsAddresses() const { return addresses_with_failover; }

        const ShardInfo &getAnyShardInfo() const {
            if (shards_info.empty())
                throw Exception("Cluster is empty", ErrorCodes::LOGICAL_ERROR);
            return shards_info.front();
        }

        /// The number of remote shards.
        size_t getRemoteShardCount() const { return remote_shard_count; }

        /// The number of clickhouse nodes located locally. 本地clickhouse节点的数量
        /// we access the local nodes directly.             可以直接访问本地节点
        size_t getLocalShardCount() const { return local_shard_count; }

        /// The number of all shards.
        size_t getShardCount() const { return shards_info.size(); }

        /// Get a subcluster consisting of one shard - index by count (from 0) of the shard of this cluster.
        std::unique_ptr<Cluster> getClusterWithSingleShard(size_t index) const;

        /// Get a subcluster consisting of one or multiple shards - indexes by count (from 0) of the shard of this cluster.
        std::unique_ptr<Cluster> getClusterWithMultipleShards(const std::vector<size_t> &indices) const;

    private:
        using SlotToShard = std::vector<UInt64>;
        SlotToShard slot_to_shard;

    public:
        const SlotToShard &getSlotToShard() const { return slot_to_shard; }

    private:
        void initMisc();

        /// For getClusterWithMultipleShards implementation.
        Cluster(const Cluster &from, const std::vector<size_t> &indices);

        String hash_of_addresses;
        /// Description of the cluster shards.
        ShardsInfo shards_info;
        /// Any remote shard.
        ShardInfo *any_remote_shard_info = nullptr;

        /// Non-empty is either addresses or addresses_with_failover.
        /// The size and order of the elements in the corresponding array corresponds to shards_info.

        /// An array of shards. For each shard, an array of replica addresses (servers that are considered identical).
        //保存着shard的数组
        //对于每个分片, 保存着副本地址的数组
        AddressesWithFailover addresses_with_failover;

        size_t remote_shard_count = 0;
        size_t local_shard_count = 0;
    };

    using ClusterPtr = std::shared_ptr<Cluster>;


    class Clusters {
    public:
        Clusters(const Poco::Util::AbstractConfiguration &config, const Settings &settings,
                 const String &config_name = "remote_servers");

        Clusters(const Clusters &) = delete;

        Clusters &operator=(const Clusters &) = delete;

        ClusterPtr getCluster(const std::string &cluster_name) const;

        void setCluster(const String &cluster_name, const ClusterPtr &cluster);

        void updateClusters(const Poco::Util::AbstractConfiguration &config, const Settings &settings,
                            const String &config_name);

    public:
        using Impl = std::map<String, ClusterPtr>;

        Impl getContainer() const;

    protected:
        Impl impl;
        mutable std::mutex mutex;
    };

    using ClustersPtr = std::shared_ptr<Clusters>;

}
