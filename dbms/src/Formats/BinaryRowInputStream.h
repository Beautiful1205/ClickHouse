#pragma once

#include <Formats/IRowInputStream.h>
#include <Core/Block.h>


namespace DB
{

class ReadBuffer;


/** A stream for inputting data in a binary line-by-line format.
  * 以二进制逐行格式输入数据的流
  */
class BinaryRowInputStream : public IRowInputStream
{
public:
    BinaryRowInputStream(ReadBuffer & istr_, const Block & sample_, bool with_names_, bool with_types_);

    bool read(MutableColumns & columns, RowReadExtension &) override;
    void readPrefix() override;

private:
    ReadBuffer & istr;
    Block header;
    bool with_names;
    bool with_types;
};

}
