#pragma once

#include <Core/Block.h>
#include <Formats/IRowInputStream.h>
#include <Formats/FormatSettings.h>


namespace DB
{

class Context;
class ReadBuffer;


/** Stream to read data in VALUES format (as in INSERT query).
  * 以 VALUES 格式读取数据的流 (用于insert语句)
  */
class ValuesRowInputStream : public IRowInputStream
{
public:
    /** Data is parsed using fast, streaming parser.
      * If interpret_expressions is true, it will, in addition, try to use SQL parser and interpreter
      *  in case when streaming parser could not parse field (this is very slow).
      */
    ValuesRowInputStream(ReadBuffer & istr_, const Block & header_, const Context & context_, const FormatSettings & format_settings);

    bool read(MutableColumns & columns, RowReadExtension &) override;

private:
    ReadBuffer & istr;
    Block header;
    std::unique_ptr<Context> context;   /// pimpl
    const FormatSettings format_settings;
};

}
