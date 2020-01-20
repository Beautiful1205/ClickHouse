#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class MergeTreeReader;
class UncompressedCache;
class MarkCache;


/// Base class for MergeTreeThreadSelectBlockInputStream and MergeTreeSelectBlockInputStream
//  如果需要只需要从一个part中读取数据, 将使用MergeTreeSelectBlockInputStream
//  如果需要从多个part中读取数据, 将使用MergeTreeThreadSelectBlockInputStream
//NOTE: MergeTreeThreadSelectBlockInputStream和MergeTreeSelectBlockInputStream都是继承了MergeTreeBaseSelectBlockInputStream中的readImpl()方法
class MergeTreeBaseSelectBlockInputStream : public IBlockInputStream
{
public:
    MergeTreeBaseSelectBlockInputStream(
        const MergeTreeData & storage,
        const PrewhereInfoPtr & prewhere_info,
        UInt64 max_block_size_rows,
        UInt64 preferred_block_size_bytes,
        UInt64 preferred_max_column_in_block_size_bytes,
        UInt64 min_bytes_to_use_direct_io,
        UInt64 max_read_buffer_size,
        bool use_uncompressed_cache,
        bool save_marks_in_cache = true,
        const Names & virt_column_names = {});

    ~MergeTreeBaseSelectBlockInputStream() override;

    static void executePrewhereActions(Block & block, const PrewhereInfoPtr & prewhere_info);

protected:
    Block readImpl() final;//final表示这个方法不能被子类重写

    /// Creates new this->task, and initilizes readers
    virtual bool getNewTask() = 0;

    /// We will call progressImpl manually.
    void progress(const Progress &) override {}

    Block readFromPart();

    void injectVirtualColumns(Block & block) const;

protected:
    const MergeTreeData & storage;

    PrewhereInfoPtr prewhere_info;

    UInt64 max_block_size_rows;
    UInt64 preferred_block_size_bytes;
    UInt64 preferred_max_column_in_block_size_bytes;

    UInt64 min_bytes_to_use_direct_io;
    UInt64 max_read_buffer_size;

    bool use_uncompressed_cache;
    bool save_marks_in_cache;

    Names virt_column_names;

    std::unique_ptr<MergeTreeReadTask> task;

    std::shared_ptr<UncompressedCache> owned_uncompressed_cache;
    std::shared_ptr<MarkCache> owned_mark_cache;

    using MergeTreeReaderPtr = std::unique_ptr<MergeTreeReader>;
    MergeTreeReaderPtr reader;
    MergeTreeReaderPtr pre_reader;
};

}
