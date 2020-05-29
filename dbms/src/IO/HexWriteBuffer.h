#pragma once

#include <IO/WriteBuffer.h>


/// Since HexWriteBuffer is often created in the inner loop, we'll make its buffer size small.
#define DBMS_HEX_WRITE_BUFFER_SIZE 32


namespace DB
{

/** Everything that is written into it, translates to HEX (in capital letters) and writes to another WriteBuffer.
  * 写到HexWriteBuffer里的数据都会被翻译成十六进制（大写字母）, 并被写到另一个WriteBuffer
  */
class HexWriteBuffer final : public WriteBuffer
{
protected:
    char buf[DBMS_HEX_WRITE_BUFFER_SIZE]; //-V730
    WriteBuffer & out;

    void nextImpl() override;

public:
    HexWriteBuffer(WriteBuffer & out_) : WriteBuffer(buf, sizeof(buf)), out(out_) {}
    ~HexWriteBuffer() override;
};

}
