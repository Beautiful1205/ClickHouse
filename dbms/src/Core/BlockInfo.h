#pragma once

#include <unordered_map>

#include <Core/Types.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** More information about the block.
  * 存储block的相关信息
  */
struct BlockInfo
{
    /** is_overflows: 是否溢出
      * After running GROUP BY ... WITH TOTALS with the max_rows_to_group_by and group_by_overflow_mode = 'any' settings,
      *  a row is inserted in the separate block with aggregated values that have not passed max_rows_to_group_by.
      * If it is such a block, then is_overflows is set to true for it.
      */
    //is_overflows这个参数: 在group_by_overflow_mode = 'any'并指定了max_rows_to_group_by的情况下,
    //                    执行GROUP BY ... WITH TOTALS子句的时候, 会创建一个新的block, 并将聚合结果插入到该block中, 但是并没有把max_rows_to_group_by这个参数传递给该block.
    //                    对于这样的block, 需要设置is_overflows=true

    /** bucket_num: 桶号
      * When using the two-level aggregation method, data with different key groups are scattered across different buckets.
      * In this case, the bucket number is indicated here. It is used to optimize the merge for distributed aggregation.
      * Otherwise -1.
      */
    //bucket_num这个参数: 使用两级聚合方法时, 具有不同key的数据分散存储在不同的桶中. 在这种情况下, 桶号保存在这里, 用于优化分布式聚合的合并.
    //                   如果使用单级聚合, bucket_num = -1
// 四个参数的含义(TYPE, NAME, DEFAULT, FIELD_NUM)
#define APPLY_FOR_BLOCK_INFO_FIELDS(M) \
    M(bool,     is_overflows,     false,     1) \
    M(Int32,    bucket_num,     -1,     2)

#define DECLARE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
    TYPE NAME = DEFAULT;

    APPLY_FOR_BLOCK_INFO_FIELDS(DECLARE_FIELD)

#undef DECLARE_FIELD

    /// Write the values in binary form. NOTE: You could use protobuf, but it would be overkill for this case.
    void write(WriteBuffer & out) const;

    /// Read the values in binary form.
    void read(ReadBuffer & in);
};

/// Block extention to support delayed defaults. AddingDefaultsBlockInputStream uses it to replace missing values with column defaults.
class BlockMissingValues
{
public:
    using RowsBitMask = std::vector<bool>; /// a bit per row for a column

    const RowsBitMask & getDefaultsBitmask(size_t column_idx) const;
    void setBit(size_t column_idx, size_t row_idx);
    bool empty() const { return rows_mask_by_column_id.empty(); }
    size_t size() const { return rows_mask_by_column_id.size(); }
    void clear() { rows_mask_by_column_id.clear(); }

private:
    using RowsMaskByColumnId = std::unordered_map<size_t, RowsBitMask>;

    /// If rows_mask_by_column_id[column_id][row_id] is true related value in Block should be replaced with column default.
    /// It could contain less columns and rows then related block.
    RowsMaskByColumnId rows_mask_by_column_id;
};

}
