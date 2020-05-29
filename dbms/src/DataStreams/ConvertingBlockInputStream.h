#pragma once

#include <unordered_map>
#include <DataStreams/IBlockInputStream.h>


namespace DB
{

/** Convert one block structure to another: 转换block的结构
  *
  * Leaves only necessary columns;
  *
  * Columns are searched in source first by name;
  *  and if there is no column with same name, then by position.
  *
  * Converting types of matching columns (with CAST function).
  *
  * Materializing columns which are const in source and non-const in result,
  *  throw if they are const in result and non const in source,
  *   or if they are const and have different values.
  */
class ConvertingBlockInputStream : public IBlockInputStream
{
public:
    enum class MatchColumnsMode
    {
        /// Require same number of columns in source and result. Match columns by corresponding positions, regardless to names.
        Position, //表示这种模式下, 互相UNION的结果列的数量要一致(列名不需要一致)
        /// Find columns in source by their names. Allow excessive columns in source.
        Name      //表示这种模式下, 互相UNION的结果列的名称要一致
    };

    ConvertingBlockInputStream(
        const Context & context,
        const BlockInputStreamPtr & input,
        const Block & result_header,
        MatchColumnsMode mode);

    String getName() const override { return "Converting"; }
    Block getHeader() const override { return header; }

private:
    Block readImpl() override;

    const Context & context;
    Block header;

    /// How to construct result block. Position in source block, where to get each column.
    using Conversion = std::vector<size_t>;
    Conversion conversion;
};

}
