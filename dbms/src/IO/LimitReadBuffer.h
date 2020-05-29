#pragma once

#include <Core/Types.h>
#include <IO/ReadBuffer.h>


namespace DB
{

/** Allows to read from another ReadBuffer no more than the specified number of bytes.
  * Note that the nested ReadBuffer may read slightly more data internally to fill its buffer.
  *
  * 允许从另一个ReadBuffer读取不超过指定字节数的数据
  * 注意，嵌套的read buffer可能会在内部读取更多的数据以填充其缓冲区。
  */
class LimitReadBuffer : public ReadBuffer
{
private:
    ReadBuffer & in;
    UInt64 limit;
    bool throw_exception;
    std::string exception_message;

    bool nextImpl() override;

public:
    LimitReadBuffer(ReadBuffer & in, UInt64 limit, bool throw_exception, std::string exception_message = {});
    ~LimitReadBuffer() override;
};

}
