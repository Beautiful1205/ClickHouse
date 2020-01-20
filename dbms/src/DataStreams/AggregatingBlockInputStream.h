#pragma once

#include <Interpreters/Aggregator.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <DataStreams/IBlockInputStream.h>


namespace DB
{


/** Aggregates the stream of blocks using the specified key columns and aggregate functions.
  * Columns with aggregate functions adds to the end of the block.
  * If final = false, the aggregate functions are not finalized, that is, they are not replaced by their value, but contain an intermediate state of calculations.
  * This is necessary so that aggregation can continue (for example, by combining streams of partially aggregated data).
  */

/** 使用指定的键列和聚合函数将数据块流聚合.
  * 具有聚合函数的列将添加到块的末尾.
  * 如果final=false, 则聚合函数不会最终确定, 也就是说, 它们不会被其值替换, 而是包含计算的中间状态.
  * 这是必要的, 以便可以继续聚合（例如, 通过组合部分聚合的数据流）.
  */
class AggregatingBlockInputStream : public IBlockInputStream
{
public:
    /** keys are taken from the GROUP BY part of the query,
      * Aggregate functions are searched everywhere in the expression.
      * Columns corresponding to keys and arguments of aggregate functions must already be computed.
      */
    /**关键字是从GROUP BY表达式中获取的, 聚合函数将在表达式中的任何位置进行搜索.
     * 聚合函数的keys和arguments对应的列必须是已经提前计算好了的.
     */
    AggregatingBlockInputStream(const BlockInputStreamPtr & input, const Aggregator::Params & params_, bool final_)
        : params(params_), aggregator(params), final(final_)
    {
        children.push_back(input);
    }

    String getName() const override { return "Aggregating"; }

    Block getHeader() const override;

protected:
    Block readImpl() override;

    Aggregator::Params params;
    Aggregator aggregator;
    bool final;

    bool executed = false;

    /// To read the data that was flushed into the temporary data file.
    struct TemporaryFileStream
    {
        ReadBufferFromFile file_in;
        CompressedReadBuffer compressed_in;
        BlockInputStreamPtr block_in;

        TemporaryFileStream(const std::string & path);
    };
    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;

     /** From here we will get the completed blocks after the aggregation. */
    std::unique_ptr<IBlockInputStream> impl;//通过调用impl->read()获取到完成聚合的blocks

    Logger * log = &Logger::get("AggregatingBlockInputStream");
};

}
