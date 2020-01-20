#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/SelectQueryInfo.h>
#include <mutex>


namespace DB
{

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;

/**   Provides read tasks for MergeTreeThreadSelectBlockInputStream`s in fine-grained batches, allowing for more
 *    uniform distribution of work amongst multiple threads.
 *    All parts and their ranges are divided into `threads` workloads with at most `sum_marks / threads` marks.
 *    Then, threads are performing reads from these workloads in "sequential" manner, requesting work in small batches.
 *    As soon as some thread has exhausted it's workload,
 *    it either is signaled that no more work is available (`do_not_steal_tasks == false`) or
 *    continues taking small batches from other threads' workloads (`do_not_steal_tasks == true`).
 */
/**为MergeTreeThreadSelectBlockInputStream需要读取的每个part创建task, 使多个线程之间更均匀地分配工作
  * 所有的part及其ranges以线程工作负载的方式(workloads)被划分到每个线程上, 每个线程最多处理(sum_marks/threads)个mark
  * 然后, 线程顺序地执行这些工作负载, 以小批量请求工作方式读取数据
  * 一旦某个线程耗尽了它的工作负载, 就会发出信号, 表明没有更多的工作可用（`do_not_steal_tasks==false`）,
  * 或者继续从其他线程的工作负载中进行小批量处理（`do_not_steal_tasks==true`）
  */

class MergeTreeReadPool : private boost::noncopyable
{
public:
    /** Pull could dynamically lower (backoff) number of threads, if read operation are too slow.
      * Settings for that backoff.
      */
    struct BackoffSettings
    {
        /// Pay attention only to reads, that took at least this amount of time. If set to 0 - means backoff is disabled.
        size_t min_read_latency_ms = 1000;
        /// Count events, when read throughput is less than specified bytes per second.
        size_t max_throughput = 1048576;
        /// Do not pay attention to event, if not enough time passed since previous event.
        size_t min_interval_between_events_ms = 1000;
        /// Number of events to do backoff - to lower number of threads in pool.
        size_t min_events = 2;

        /// Constants above is just an example.
        BackoffSettings(const Settings & settings)
            : min_read_latency_ms(settings.read_backoff_min_latency_ms.totalMilliseconds()),
            max_throughput(settings.read_backoff_max_throughput),
            min_interval_between_events_ms(settings.read_backoff_min_interval_between_events_ms.totalMilliseconds()),
            min_events(settings.read_backoff_min_events)
        {
        }

        BackoffSettings() : min_read_latency_ms(0) {}
    };

    BackoffSettings backoff_settings;

private:
    /** State to track numbers of slow reads.
      */
    struct BackoffState
    {
        size_t current_threads;
        Stopwatch time_since_prev_event {CLOCK_MONOTONIC_COARSE};
        size_t num_events = 0;

        BackoffState(size_t threads) : current_threads(threads) {}
    };

    BackoffState backoff_state;

public:
    MergeTreeReadPool(
        const size_t threads, const size_t sum_marks, const size_t min_marks_for_concurrent_read,
        RangesInDataParts parts, const MergeTreeData & data, const PrewhereInfoPtr & prewhere_info,
        const bool check_columns, const Names & column_names,
        const BackoffSettings & backoff_settings, size_t preferred_block_size_bytes,
        const bool do_not_steal_tasks = false);

    MergeTreeReadTaskPtr getTask(const size_t min_marks_to_read, const size_t thread, const Names & ordered_names);

    /** Each worker could call this method and pass information about read performance.
      * If read performance is too low, pool could decide to lower number of threads: do not assign more tasks to several threads.
      * This allows to overcome excessive load to disk subsystem, when reads are not from page cache.
      */
    void profileFeedback(const ReadBufferFromFileBase::ProfileInfo info);

    /// This method tells which mark ranges we have to read if we start from @from mark range
    MarkRanges getRestMarks(const std::string & part_path, const MarkRange & from) const;

    Block getHeader() const;

private:
    std::vector<size_t> fillPerPartInfo(
        RangesInDataParts & parts, const bool check_columns);

    void fillPerThreadInfo(
        const size_t threads, const size_t sum_marks, std::vector<size_t> per_part_sum_marks,
        RangesInDataParts & parts, const size_t min_marks_for_concurrent_read);

    std::vector<std::shared_lock<std::shared_mutex>> per_part_columns_lock;
    const MergeTreeData & data;
    Names column_names;
    bool do_not_steal_tasks;
    bool predict_block_size_bytes;
    std::vector<NameSet> per_part_column_name_set;
    std::vector<NamesAndTypesList> per_part_columns;
    std::vector<NamesAndTypesList> per_part_pre_columns;
    std::vector<char> per_part_should_reorder;
    std::vector<MergeTreeBlockSizePredictorPtr> per_part_size_predictor;
    PrewhereInfoPtr prewhere_info;

    struct Part
    {
        MergeTreeData::DataPartPtr data_part;
        size_t part_index_in_query;
    };

    std::vector<Part> parts_with_idx;

    struct ThreadTask
    {
        struct PartIndexAndRange
        {
            size_t part_idx;
            MarkRanges ranges;
        };

        std::vector<PartIndexAndRange> parts_and_ranges;
        std::vector<size_t> sum_marks_in_parts;
    };

    std::vector<ThreadTask> threads_tasks;

    std::set<size_t> remaining_thread_tasks;

    RangesInDataParts parts_ranges;

    mutable std::mutex mutex;

    Logger * log = &Logger::get("MergeTreeReadPool");
};

using MergeTreeReadPoolPtr = std::shared_ptr<MergeTreeReadPool>;

}
