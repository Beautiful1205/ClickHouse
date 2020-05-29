#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/copyData.h>


namespace DB
{

class IBlockOutputStream;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;


/** An empty stream of blocks.
  * But at the first read attempt, copies the data from the passed `input` to the `output`.
  * This is necessary to execute the query INSERT SELECT - the query copies data, but returns nothing.
  * The query could be executed without wrapping it in an empty BlockInputStream,
  *  but the progress of query execution and the ability to cancel the query would not work.
  */
/** 空的stream
  * 但在第一次尝试读取时, 会将数据从input复制到output;
  * 这是执行查询INSERT SELECT所必需的: 这个query执行过程中会复制数据, 但不返回
  * 不必将这个query封装在空的BlockInputStream中, 这个query也能执行,
  * 但是这个query执行的进度将不显示, 也不能取消query.
  */
class NullAndDoCopyBlockInputStream : public IBlockInputStream
{
public:
    NullAndDoCopyBlockInputStream(const BlockInputStreamPtr & input_, BlockOutputStreamPtr output_)
        : input(input_), output(output_)
    {
        children.push_back(input_);
    }

    /// Suppress readPrefix and readSuffix, because they are called by copyData.
    void readPrefix() override {}
    void readSuffix() override {}

    String getName() const override { return "NullAndDoCopy"; }

    Block getHeader() const override { return {}; }

protected:
    Block readImpl() override
    {
        copyData(*input, *output);
        return Block();
    }

private:
    BlockInputStreamPtr input;
    BlockOutputStreamPtr output;
};

}
