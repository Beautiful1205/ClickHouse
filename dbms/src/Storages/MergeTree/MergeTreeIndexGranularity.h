#pragma once

#include <vector>
#include <Storages/MergeTree/MarkRange.h>

namespace DB {

/// Class contains information about index granularity in rows of MergeTreeDataPart
/// Inside it contains vector of partial sums of rows after mark:
/// |-----|---|----|----|
/// |  5  | 8 | 12 | 16 |
/// If user doesn't specify setting adaptive_index_granularity_bytes for MergeTree* table,
/// all values in inner vector would have constant stride (default 8192).
    class MergeTreeIndexGranularity {
    private:
        //每个part内, mark之间的行数.
        std::vector<size_t> marks_rows_partial_sums; //针对注释中的例子, 结果就是vector[5,8,12,16].
        // 所以如本类中getMarkRows()方法所示, 如果想获取某个mark到下一个mark之间的数据行数.
        // 如果是第一个mark, 就返回vector[0]; 如果是别的mark, 就返回vector[index] - vector[index - 1];

        bool initialized = false;

    public:
        MergeTreeIndexGranularity() = default;

        explicit MergeTreeIndexGranularity(const std::vector<size_t> &marks_rows_partial_sums_);

        MergeTreeIndexGranularity(size_t marks_count, size_t fixed_granularity);

        /// Return count of rows between marks
        size_t getRowsCountInRange(const MarkRange &range) const;

        /// Return count of rows between marks
        size_t getRowsCountInRange(size_t begin, size_t end) const;

        /// Return sum of rows between all ranges
        size_t getRowsCountInRanges(const MarkRanges &ranges) const;

        /// Return amount of marks that contains amount of `number_of_rows` starting from `from_mark` and possible some offset_in_rows from `from_mark`
        ///                                     1    2  <- answer
        /// |-----|---------------------------|----|----|
        ///       ^------------------------^-----------^
        ////  from_mark  offset_in_rows    number_of_rows
        size_t countMarksForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows = 0) const;

        /// Total marks
        size_t getMarksCount() const;

        /// Total rows
        size_t getTotalRows() const;

        /// Rows after mark to next mark
        inline size_t getMarkRows(size_t mark_index) const {
            if (mark_index == 0)
                return marks_rows_partial_sums[0];
            else
                return marks_rows_partial_sums[mark_index] - marks_rows_partial_sums[mark_index - 1];
        }

        /// Return amount of rows before mark
        size_t getMarkStartingRow(size_t mark_index) const;

        /// Amount of rows after last mark
        size_t getLastMarkRows() const {
            size_t last = marks_rows_partial_sums.size() - 1;
            return getMarkRows(last);
        }

        bool empty() const {
            return marks_rows_partial_sums.empty();
        }

        bool isInitialized() const {
            return initialized;
        }

        void setInitialized() {
            initialized = true;
        }

        /// Add new mark with rows_count
        void appendMark(size_t rows_count);

        /// Add `size` of marks with `fixed_granularity` rows
        void resizeWithFixedGranularity(size_t size, size_t fixed_granularity);
    };

}
