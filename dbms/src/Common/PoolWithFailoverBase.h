#pragma once

#include <time.h>
#include <cstdlib>
#include <climits>
#include <random>
#include <functional>
#include <common/Types.h>
#include <ext/scope_guard.h>
#include <Core/Types.h>
#include <Common/PoolBase.h>
#include <Common/ProfileEvents.h>
#include <Common/NetException.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>


namespace DB {
    namespace ErrorCodes {
        extern const int ALL_CONNECTION_TRIES_FAILED;
        extern const int ALL_REPLICAS_ARE_STALE;
        extern const int LOGICAL_ERROR;
    }
}

namespace ProfileEvents {
    extern const Event DistributedConnectionFailTry;
    extern const Event DistributedConnectionFailAtAll;
}

/// This class provides a pool with fault tolerance. It is used for pooling of connections to replicated DB.
/// Initialized by several PoolBase objects.

/// When a connection is requested, tries to create or choose an alive connection from one of the nested pools.
/// Pools are tried in the order consistent with lexicographical order of (error count, priority, random number) tuples.

/// Number of tries for a single pool is limited by max_tries parameter.
/// The client can set nested pool priority by passing a GetPriority functor.
///
/// NOTE: if one of the nested pools blocks because it is empty, this pool will also block.
///
/// The client must provide a TryGetEntryFunc functor, which should perform a single try to get a connection from a nested pool.
/// This functor can also check if the connection satisfies some eligibility criterion (e.g. check if the replica is up-to-date).

///提供具有容错性的连接池, 用于连接到包含副本的数据库。
///由多个PoolBase对象初始化。

///当请求连接时, 尝试新建连接或从一个嵌套池中选择可用的连接。
///按照字典序(错误计数、优先级、随机数)将所有的连接池排序, 逐一进行尝试连接。

///对于每个连接池, 最多进行 max_tries 次尝试连接
///客户端可以通过传递GetPriority函数来设置嵌套池的优先级。

///注意: 如果其中一个嵌套池因为空而被阻塞, 则此连接池也将被阻塞。

///客户端必须提供TryGetEntryFunc函数，该函数应执行一次尝试以从嵌套池获取连接。此函数还可以用来检查连接是否满足某些合格条件(例如检查副本是否为最新的)。

//继承boost::noncopyable, 使此类不可被拷贝
template<typename TNestedPool>
class PoolWithFailoverBase : private boost::noncopyable {
public:
    using NestedPool = TNestedPool;
    using NestedPoolPtr = std::shared_ptr<NestedPool>;
    using Entry = typename NestedPool::Entry;
    using NestedPools = std::vector<NestedPoolPtr>;

    PoolWithFailoverBase(
            NestedPools nested_pools_,
            size_t max_tries_,
            time_t decrease_error_period_,
            Logger *log_)
            : nested_pools(std::move(nested_pools_)), max_tries(max_tries_),
              decrease_error_period(decrease_error_period_), shared_pool_states(nested_pools.size()), log(log_) {
    }

    struct TryResult {
        TryResult() = default;

        explicit TryResult(Entry entry_)
                : entry(std::move(entry_)), is_usable(true), is_up_to_date(true) {
        }

        void reset() {
            entry = Entry();
            is_usable = false;
            is_up_to_date = false;
            staleness = 0.0;
        }

        Entry entry;
        bool is_usable = false; /// If false, the entry is unusable for current request 对当前连接请求不可用, 但可能对别的连接请求可用, 所以此时不会将错误计数增加
                                /// (but may be usable for other requests, so error counts are not incremented)
        bool is_up_to_date = false; /// If true, the entry is a connection to up-to-date replica.                true表示entry连接的是最新的副本
        double staleness = 0.0; /// Helps choosing the "least stale" option when all replicas are stale.         如果所有的副本都不是最新的, 则根据staleness选择比较新的
    };

    /// This functor must be provided by a client.
    /// It must perform a single try that takes a connection from the provided pool and checks that it is good.
    using TryGetEntryFunc = std::function<TryResult(NestedPool &pool, std::string &fail_message)>;

    /// The client can provide this functor to affect load balancing - the index of a pool is passed to this functor.
    /// The pools with lower result value will be tried first.
    // 参数index 表示的是pool的编号
    // index越小优先级越高
    using GetPriorityFunc = std::function<size_t(size_t index)>;

    /// Returns a single connection.
    Entry get(const TryGetEntryFunc &try_get_entry, const GetPriorityFunc &get_priority = GetPriorityFunc());


    /// Returns at least min_entries and at most max_entries connections (at most one connection per nested pool).
    /// The method will throw if it is unable to get min_entries alive connections or
    /// if fallback_to_stale_replicas is false and it is unable to get min_entries connections to up-to-date replicas.
    // 最少返回min_entries个连接, 最多返回max_entries个连接. (每个nested pool最多一个连接)
    //如果返回的连接数小于min_entries 或 fallback_to_stale_replicas = false 表示无法获取到最新副本的连接, 该方法将抛出异常
    std::vector<TryResult> getMany(
            size_t min_entries, size_t max_entries,
            const TryGetEntryFunc &try_get_entry,
            const GetPriorityFunc &get_priority = GetPriorityFunc(),
            bool fallback_to_stale_replicas = true);

    void reportError(const Entry &entry);

protected:
    struct PoolState;

    using PoolStates = std::vector<PoolState>;

    /// This function returns a copy of pool states to avoid race conditions when modifying shared pool states.
    //此函数返回连接池状态的副本, 以避免修改共享池状态时出现竞争的情况
    PoolStates updatePoolStates();

    NestedPools nested_pools;

    const size_t max_tries;

    const time_t decrease_error_period;

    std::mutex pool_states_mutex;
    PoolStates shared_pool_states;
    /// The time when error counts were last decreased.
    // 上次减少错误计数的时间
    time_t last_error_decrease_time = 0;

    Logger *log;
};

template<typename TNestedPool>
typename TNestedPool::Entry
PoolWithFailoverBase<TNestedPool>::get(const TryGetEntryFunc &try_get_entry, const GetPriorityFunc &get_priority) {
    std::vector<TryResult> results = getMany(1, 1, try_get_entry, get_priority);
    if (results.empty() || results[0].entry.isNull())
        throw DB::Exception(
                "PoolWithFailoverBase::getMany() returned less than min_entries entries.",
                DB::ErrorCodes::LOGICAL_ERROR);
    return results[0].entry;
}

//这个方法是比较基础的方法
template<typename TNestedPool>
std::vector<typename PoolWithFailoverBase<TNestedPool>::TryResult>
PoolWithFailoverBase<TNestedPool>::getMany(
        size_t min_entries, size_t max_entries,
        const TryGetEntryFunc &try_get_entry,
        const GetPriorityFunc &get_priority,
        bool fallback_to_stale_replicas) {
    /// Update random numbers and error counts.
    PoolStates pool_states = updatePoolStates();//得到所有连接池的状态(是副本, 详见第131行)

    //设置PoolStates里每个PoolState的优先级
    //load_balancing = RANDOM, 所以这个get_priority对所有的连接池应该都一样
    if (get_priority) {
        for (size_t i = 0; i < pool_states.size(); ++i)
            pool_states[i].priority = get_priority(i);
    }

    struct ShuffledPool {
        NestedPool *pool;
        const PoolState *state;
        size_t index;
        size_t error_count = 0;
    };

    /// Sort the pools into order in which they will be tried (based on respective PoolStates).
    //根据nested_pools和pool_states构建ShuffledPool
    std::vector<ShuffledPool> shuffled_pools;
    shuffled_pools.reserve(nested_pools.size());
    for (size_t i = 0; i < nested_pools.size(); ++i)
        shuffled_pools.push_back(ShuffledPool{nested_pools[i].get(), &pool_states[i], i, 0});

    //将ShuffledPool中的元素排序(按照PoolState将多个NestedPool排序)
    //根据error_count、priority、random比较两个PoolState. 优先选错误少的, 然后是优先级高的, 最后随机数
    std::sort(
            shuffled_pools.begin(), shuffled_pools.end(),
            [](const ShuffledPool &lhs, const ShuffledPool &rhs) {
                return PoolState::compare(*lhs.state, *rhs.state);
            });

    /// We will try to get a connection from each pool until a connection is produced or max_tries is reached.
    //从每个连接池获取一个连接. (成功获取到一个连接后停止 或者 尝试最大次数后停止)
    std::vector<TryResult> try_results(shuffled_pools.size());
    size_t entries_count = 0;
    size_t usable_count = 0;
    size_t up_to_date_count = 0;
    size_t failed_pools_count = 0;

    /// At exit update shared error counts with error counts occured during this call.
    SCOPE_EXIT(
            {
                std::lock_guard lock(pool_states_mutex);
                for (const ShuffledPool &pool: shuffled_pools)
                    shared_pool_states[pool.index].error_count += pool.error_count;
            });

    std::string fail_messages;
    bool finished = false;
    while (!finished) {
        for (size_t i = 0; i < shuffled_pools.size(); ++i) {
            if (up_to_date_count >= max_entries /// Already enough good entries. 已经找到合适的entry
                || entries_count + failed_pools_count >=
                   nested_pools.size()) /// No more good entries will be produced. 没有合适的entry
            {
                finished = true;
                break;
            }

            ShuffledPool &shuffled_pool = shuffled_pools[i];
            TryResult &result = try_results[i];
            //还没有构造TryResult, 如果此时已经有当前这个连接池的error_count>=max_tries 或者 result.entry.isNull()=false, 忽略这个连接池
            if (shuffled_pool.error_count >= max_tries ||
                !result.entry.isNull())
                continue;

            //表示当前这个连接池的错误数<max_tries 且 result.entry.isNull()=true(因为还没构造, 所以连接为空)
            std::string fail_message;
            //根据pool和fail_message构造TryResult
            result = try_get_entry(*shuffled_pool.pool, fail_message);

            if (!fail_message.empty())
                fail_messages += fail_message + '\n';

            //针对新构造的TryResult，此时result.entry.isNull()=false
            if (!result.entry.isNull()) {
                ++entries_count;
                if (result.is_usable) {
                    ++usable_count;
                    if (result.is_up_to_date)
                        ++up_to_date_count;
                }
            } else {
                LOG_WARNING(log, "Connection failed at try №"
                        << (shuffled_pool.error_count + 1) << ", reason: " << fail_message);
                ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

                ++shuffled_pool.error_count;

                if (shuffled_pool.error_count >= max_tries) {
                    ++failed_pools_count;
                    ProfileEvents::increment(ProfileEvents::DistributedConnectionFailAtAll);
                }
            }
        }
    }

    if (usable_count < min_entries)
        throw DB::NetException(
                "All connection tries failed. Log: \n\n" + fail_messages + "\n",
                DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED);

    //把为空或者不可用的连接从try_results中剔除
    try_results.erase(
            std::remove_if(
                    try_results.begin(), try_results.end(),
                    [](const TryResult &r) { return r.entry.isNull() || !r.is_usable; }),
            try_results.end());

    /// Sort so that preferred items are near the beginning.
    //根据规则排序: 先根据是否为最新的副本判断, 如果不能比较出结果在根据staleness判断哪个是比较新的
    //最新的排在前面, 按数据新旧顺序往后排
    std::stable_sort(
            try_results.begin(), try_results.end(),
            [](const TryResult &left, const TryResult &right) {
                return std::forward_as_tuple(!left.is_up_to_date, left.staleness)
                       < std::forward_as_tuple(!right.is_up_to_date, right.staleness);
            });

    if (up_to_date_count >= min_entries) {
        /// There is enough up-to-date entries.
        try_results.resize(up_to_date_count);//只保留前up_to_date_count个, 其余的删掉
    } else if (fallback_to_stale_replicas) {
        /// There is not enough up-to-date entries but we are allowed to return stale entries.
        /// Gather all up-to-date ones and least-bad stale ones.

        size_t size = std::min(try_results.size(), max_entries);
        try_results.resize(size);
    } else
        throw DB::Exception(
                "Could not find enough connections to up-to-date replicas. Got: " + std::to_string(up_to_date_count)
                + ", needed: " + std::to_string(min_entries),
                DB::ErrorCodes::ALL_REPLICAS_ARE_STALE);

    return try_results;
}

template<typename TNestedPool>
void PoolWithFailoverBase<TNestedPool>::reportError(const Entry &entry) {
    for (size_t i = 0; i < nested_pools.size(); ++i) {
        if (nested_pools[i]->contains(entry)) {
            std::lock_guard lock(pool_states_mutex);
            ++shared_pool_states[i].error_count;
            return;
        }
    }
    throw DB::Exception("Can't find pool to report error", DB::ErrorCodes::LOGICAL_ERROR);
}

template<typename TNestedPool>
struct PoolWithFailoverBase<TNestedPool>::PoolState {
    UInt64 error_count = 0;
    Int64 priority = 0;
    UInt32 random = 0;

    void randomize() {
        random = rng();
    }

    //根据error_count、priority、random比较两个PoolState
    //优先选错误少的, 然后是优先级高的, 最后随机数
    static bool compare(const PoolState &lhs, const PoolState &rhs) {
        return std::forward_as_tuple(lhs.error_count, lhs.priority, lhs.random)
               < std::forward_as_tuple(rhs.error_count, rhs.priority, rhs.random);
    }

private:
    std::minstd_rand rng = std::minstd_rand(randomSeed());//一个随机数生成器
};

template<typename TNestedPool>
typename PoolWithFailoverBase<TNestedPool>::PoolStates
PoolWithFailoverBase<TNestedPool>::updatePoolStates() {
    PoolStates result;
    result.reserve(nested_pools.size());

    {
        std::lock_guard lock(pool_states_mutex);

        for (auto &state : shared_pool_states)
            state.randomize();//设置random参数

        time_t current_time = time(nullptr);

        //上次减少错误计数的时间不为空, 即已经有过减少错误计数的操作了
        if (last_error_decrease_time) {
            time_t delta = current_time - last_error_decrease_time;//一个时间间隔

            if (delta >= 0) {
                /// Divide error counts by 2 every decrease_error_period seconds.  时间间隔 除以 排错周期 = 个数
                //见配置decrease_error_period = 2 * 300s = 600s
                size_t shift_amount = delta / decrease_error_period;
                /// Update time but don't do it more often than once a period. Else if the function is called often enough, error count will never decrease.
                //错误存在的时间大于一个周期时(存在时间可能大于1个周期, 甚至是2个周期...), 更新更新一次last_error_decrease_time. 否则该函数可能被频繁调用, 错误计数将永远不会减少
                if (shift_amount)
                    last_error_decrease_time = current_time;

                //此处sizeof(UInt64) * CHAR_BIT = 64, 即如果错误存在时间已经大于等于64个周期(64 * 600s), 需要将错误数清零
                if (shift_amount >= sizeof(UInt64) * CHAR_BIT) {//错误存在的时间已经足够长了, 则将错误计数重置为0.
                    for (auto &state : shared_pool_states)
                        state.error_count = 0;
                } else if (shift_amount) {//错误存在的时间大于一个周期, 但存在的时间还不足够长, 则将错误计数减少, 新的错误数等于原错误数除以2^shift_amount.
                    for (auto &state : shared_pool_states)
                        state.error_count >>= shift_amount;//举例: error_count = 32, shift_amount = 2, 移位操作error_count >>= shift_amount后, error_count = 32/(2^2) = 8
                }
            }
        } else//还没有进行过减少错误计数的操作
            last_error_decrease_time = current_time;

        result.assign(shared_pool_states.begin(), shared_pool_states.end());//将shared_pool_states中的元素赋值到result
    }
    return result;
}
