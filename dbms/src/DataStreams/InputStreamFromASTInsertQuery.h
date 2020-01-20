#pragma once

#include <Parsers/IAST.h>
#include <DataStreams/IBlockInputStream.h>
#include <cstddef>
#include <memory>


namespace DB
{

struct BlockIO;
class Context;

/** Prepares an input stream which produce data containing in INSERT query
  * Head of inserting data could be stored in INSERT ast directly
  * Remaining (tail) data could be stored in input_buffer_tail_part
  */
/** 生成包含需要insert数据的输入流
  * 需要插入的数据的Head可以直接存储在INSERT AST中, 剩余的数据(tail)可以存储在input_buffer_tail_part中
  */
class InputStreamFromASTInsertQuery : public IBlockInputStream
{
public:
    InputStreamFromASTInsertQuery(const ASTPtr & ast, ReadBuffer * input_buffer_tail_part, const Block & header, const Context & context);

    Block readImpl() override { return res_stream->read(); }
    void readPrefixImpl() override { return res_stream->readPrefix(); }
    void readSuffixImpl() override { return res_stream->readSuffix(); }

    String getName() const override { return "InputStreamFromASTInsertQuery"; }

    Block getHeader() const override { return res_stream->getHeader(); }

private:
    std::unique_ptr<ReadBuffer> input_buffer_ast_part;
    std::unique_ptr<ReadBuffer> input_buffer_contacenated;

    BlockInputStreamPtr res_stream;
};

}
