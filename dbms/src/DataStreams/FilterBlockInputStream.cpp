#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnConst.h>
#include <Columns/FilterDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/typeid_cast.h>

#include <DataStreams/FilterBlockInputStream.h>


namespace DB {

    namespace ErrorCodes {
        extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    }


    FilterBlockInputStream::FilterBlockInputStream(const BlockInputStreamPtr &input,
                                                   const ExpressionActionsPtr &expression_,
                                                   const String &filter_column_name, bool remove_filter)
            : remove_filter(remove_filter), expression(expression_) {
        children.push_back(input);

        /// Determine position of filter column.
        header = input->getHeader();
        expression->execute(header);

        filter_column = header.getPositionByName(filter_column_name);
        auto &column_elem = header.safeGetByPosition(filter_column);

        /// Isn't the filter already constant?
        if (column_elem.column)
            constant_filter_description = ConstantFilterDescription(*column_elem.column);

        if (!constant_filter_description.always_false
            && !constant_filter_description.always_true) {
            /// Replace the filter column to a constant with value 1.
            FilterDescription filter_description_check(*column_elem.column);
            column_elem.column = column_elem.type->createColumnConst(header.rows(), 1u);
        }

        if (remove_filter)
            header.erase(filter_column_name);
    }


    String FilterBlockInputStream::getName() const { return "Filter"; }


    Block FilterBlockInputStream::getTotals() {
        totals = children.back()->getTotals();
        expression->executeOnTotals(totals);

        return totals;
    }


    Block FilterBlockInputStream::getHeader() const {
        return header;
    }


    Block FilterBlockInputStream::readImpl() {
        Block res;

        // 分析过滤的列是否为常量列, 如果是常量列, 则过滤结果始终为false或始终为true。
        if (constant_filter_description.always_false)//过滤的列是常量列且过滤结果始终为false
            return removeFilterIfNeed(std::move(res));

        /// Until non-empty block after filtering or end of stream.
        while (1) {
            res = children.back()->read();
            if (!res)
                return res;

            expression->execute(res);

            if (constant_filter_description.always_true)
                return removeFilterIfNeed(std::move(res));

            size_t columns = res.columns();//当前的Block中共有多少列
            ColumnPtr column = res.safeGetByPosition(filter_column).column;

            /** It happens that at the stage of analysis of expressions (in sample_block) the columns-constants have not been calculated yet,
                *  and now - are calculated. That is, not all cases are covered by the code above.
                * This happens if the function returns a constant for a non-constant argument.
                * For example, `ignore` function.
                */
            constant_filter_description = ConstantFilterDescription(*column);

            if (constant_filter_description.always_false) {
                res.clear();
                return res;
            }

            if (constant_filter_description.always_true)
                return removeFilterIfNeed(std::move(res));

            FilterDescription filter_and_holder(*column);

            /** Let's find out how many rows will be in result.
              * To do this, we filter out the first non-constant column or calculate number of set bytes in the filter.
              * 让我们看看结果中有多少行, 为此, 需要过滤掉第一个非常量列 或者 计算过滤器中的设置字节数
              */
            size_t first_non_constant_column = 0;//找到第一个非常量列的位置
            for (size_t i = 0; i < columns; ++i) {//遍历当前Block中的所有列
                if (!res.safeGetByPosition(i).column->isColumnConst()) {//如果当前列存储的数据不是常量
                    first_non_constant_column = i;

                    if (first_non_constant_column != static_cast<size_t>(filter_column))
                        break;
                }
            }

            size_t filtered_rows = 0;//过滤后留下的行数
            if (first_non_constant_column != static_cast<size_t>(filter_column)) {
                ColumnWithTypeAndName &current_column = res.safeGetByPosition(first_non_constant_column);
                current_column.column = current_column.column->filter(*filter_and_holder.data, -1);
                filtered_rows = current_column.column->size();
            } else {
                filtered_rows = countBytesInFilter(*filter_and_holder.data);
            }

            /// If the current block is completely filtered out, let's move on to the next one.
            // 过滤后留下的行数 = 0, 表示当前Block中的数据都不符合条件, 全部都被过滤掉了, 继续下一个块
            if (filtered_rows == 0)
                continue;

            /// If all the rows pass through the filter.
            // 过滤后留下的行数 = Block中数据的行数, 表示当前Block中的数据全部符合条件, 没有被过滤掉
            if (filtered_rows == filter_and_holder.data->size()) {
                /// Replace the column with the filter by a constant. 使用常量代替需要过滤的那一列
                res.safeGetByPosition(filter_column).column = res.safeGetByPosition(
                        filter_column).type->createColumnConst(filtered_rows, 1u);
                /// No need to touch the rest of the columns.
                return removeFilterIfNeed(std::move(res));
            }

            /// Filter the rest of the columns.
            // 0 < 过滤后留下的行数 < Block中数据的行数, 表示当前Block中的数据部分符合条件, 部分不符合条件
            for (size_t i = 0; i < columns; ++i) {
                ColumnWithTypeAndName &current_column = res.safeGetByPosition(i);

                if (i == static_cast<size_t>(filter_column)) {
                    /// The column with filter itself is replaced with a column with a constant `1`, since after filtering, nothing else will remain.
                    /// NOTE User could pass column with something different than 0 and 1 for filter.
                    /// Example:
                    ///  SELECT materialize(100) AS x WHERE x
                    /// will work incorrectly.
                    //需要过滤的列被替换为带有常量列(常量=1), 因为在过滤之后, 将不会保留其他任何内容。
                    //注意: 需要向过滤器传递非0和非1的列作为参数
                    current_column.column = current_column.type->createColumnConst(filtered_rows, 1u);
                    continue;
                }

                if (i == first_non_constant_column)
                    continue;

                if (current_column.column->isColumnConst())
                    current_column.column = current_column.column->cut(0, filtered_rows);
                else
                    current_column.column = current_column.column->filter(*filter_and_holder.data, -1);
            }

            return removeFilterIfNeed(std::move(res));
        }
    }


    Block FilterBlockInputStream::removeFilterIfNeed(Block &&block) {
        if (block && remove_filter)
            block.erase(static_cast<size_t>(filter_column));

        return std::move(block);
    }


}
