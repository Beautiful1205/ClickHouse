#include <Core/Types.h>
#include <Common/hex.h>
#include <Common/Exception.h>
#include <IO/HexWriteBuffer.h>


namespace DB
{

void HexWriteBuffer::nextImpl()
{
    if (!offset())
        return;

    for (Position p = working_buffer.begin(); p != pos; ++p)
    {
        UInt8 byte = *p;//一个字节一个字节的写, 把每个字节转成16进制数
        out.write(hexDigitUppercase(byte / 16));
        out.write(hexDigitUppercase(byte % 16));
    }
}

HexWriteBuffer::~HexWriteBuffer()
{
    try
    {
        nextImpl();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
