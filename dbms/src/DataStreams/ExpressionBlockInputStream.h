#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

class ExpressionActions;

/** Executes a certain expression over the block.
  * The expression consists of column identifiers from the block, constants, common functions.
  * For example: hits * 2 + 3, url LIKE '%yandex%'
  * The expression processes each row independently of the others.
  */
//在数据块上执行某个指定的表达式
//表达式由块中的列标识符、常量和公共函数组成
//针对每一行单独的执行这个表达式
class ExpressionBlockInputStream : public IBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
    ExpressionBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_);

    String getName() const override;
    Block getTotals() override;
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    ExpressionActionsPtr expression;
    Block cached_header;
};

}
