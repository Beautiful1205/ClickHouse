#include <DataTypes/DataTypeCustomSimpleTextSerialization.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

namespace
{
using namespace DB;

static String serializeToString(const DataTypeCustomSimpleTextSerialization & domain, const IColumn & column, size_t row_num, const FormatSettings & settings)
{
    WriteBufferFromOwnString buffer;
    domain.serializeText(column, row_num, buffer, settings);

    return buffer.str();
}

static void deserializeFromString(const DataTypeCustomSimpleTextSerialization & domain, IColumn & column, const String & s, const FormatSettings & settings)
{
    ReadBufferFromString istr(s);//把数据从str中读取到buffer中
    domain.deserializeText(column, istr, settings);// IPV4/IPV6反序列化
}

} // namespace

namespace DB
{

DataTypeCustomSimpleTextSerialization::~DataTypeCustomSimpleTextSerialization()
{
}

void DataTypeCustomSimpleTextSerialization::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeEscapedString(serializeToString(*this, column, row_num, settings), ostr);
}

void DataTypeCustomSimpleTextSerialization::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String str;
    readEscapedString(str, istr);//读取转义字符穿序列, 将数据从buffer保存到str中
    deserializeFromString(*this, column, str, settings);//将数据从str中反序列化到column中
}

void DataTypeCustomSimpleTextSerialization::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeQuotedString(serializeToString(*this, column, row_num, settings), ostr);
}

void DataTypeCustomSimpleTextSerialization::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String str;
    readQuotedString(str, istr);
    deserializeFromString(*this, column, str, settings);
}

void DataTypeCustomSimpleTextSerialization::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeCSVString(serializeToString(*this, column, row_num, settings), ostr);
}

void DataTypeCustomSimpleTextSerialization::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String str;
    readCSVString(str, istr, settings.csv);
    deserializeFromString(*this, column, str, settings);
}

void DataTypeCustomSimpleTextSerialization::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeJSONString(serializeToString(*this, column, row_num, settings), ostr, settings);
}

void DataTypeCustomSimpleTextSerialization::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String str;
    readJSONString(str, istr);
    deserializeFromString(*this, column, str, settings);
}

void DataTypeCustomSimpleTextSerialization::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    writeXMLString(serializeToString(*this, column, row_num, settings), ostr);
}

} // namespace DB
