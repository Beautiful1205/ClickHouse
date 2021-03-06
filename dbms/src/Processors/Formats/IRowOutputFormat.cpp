#include <string>
#include <Processors/Formats/IRowOutputFormat.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void IRowOutputFormat::consume(DB::Chunk chunk)
{
    writePrefixIfNot();

    auto num_rows = chunk.getNumRows();
    auto & columns = chunk.getColumns();

    for (UInt64 row = 0; row < num_rows; ++row)
    {
        if (!first_row)
            writeRowBetweenDelimiter();
        first_row = false;

        write(columns, row);

        if (write_single_row_callback)
            write_single_row_callback();
    }
}

void IRowOutputFormat::consumeTotals(DB::Chunk chunk)
{
    writePrefixIfNot();
    writeSuffixIfNot();

    auto num_rows = chunk.getNumRows();
    if (num_rows != 1)
        throw Exception("Got " + toString(num_rows) + " in totals chunk, expected 1", ErrorCodes::LOGICAL_ERROR);

    auto & columns = chunk.getColumns();

    writeBeforeTotals();
    writeTotals(columns, 0);
    writeAfterTotals();
}

void IRowOutputFormat::consumeExtremes(DB::Chunk chunk)
{
    writePrefixIfNot();
    writeSuffixIfNot();

    auto num_rows = chunk.getNumRows();
    auto & columns = chunk.getColumns();
    if (num_rows != 2)
        throw Exception("Got " + toString(num_rows) + " in extremes chunk, expected 2", ErrorCodes::LOGICAL_ERROR);

    writeBeforeExtremes();
    writeMinExtreme(columns, 0);
    writeRowBetweenDelimiter();
    writeMaxExtreme(columns, 1);
    writeAfterExtremes();
}

void IRowOutputFormat::finalize()
{
    writePrefixIfNot();
    writeSuffixIfNot();
    writeLastSuffix();
}

void IRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    size_t num_columns = columns.size();

    writeRowStartDelimiter();

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *types[i], row_num);
    }

    writeRowEndDelimiter();
}

void IRowOutputFormat::writeMinExtreme(const DB::Columns & columns, size_t row_num)
{
    write(columns, row_num);
}

void IRowOutputFormat::writeMaxExtreme(const DB::Columns & columns, size_t row_num)
{
    write(columns, row_num);
}

void IRowOutputFormat::writeTotals(const DB::Columns & columns, size_t row_num)
{
    write(columns, row_num);
}

}
