#pragma once

#include <list>

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>


namespace DB
{

/** Consequent pairs of rows: the operator and the corresponding function. For example, "+" -> "plus".
  * The parsing order of the operators is significant.
  */
using Operators_t = const char **;


/** List of elements separated by something. */
class ParserList : public IParserBase
{
public:
    ParserList(ParserPtr && elem_parser_, ParserPtr && separator_parser_, bool allow_empty_ = true)
        : elem_parser(std::move(elem_parser_)), separator_parser(std::move(separator_parser_)), allow_empty(allow_empty_)
    {
    }
protected:
    const char * getName() const { return "list of elements"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
private:
    ParserPtr elem_parser;
    ParserPtr separator_parser;
    bool allow_empty;
};


/** An expression with an infix binary left-associative operator.  左结合的二元中缀表达式
  * For example, a + b - c + d.
  */
class ParserLeftAssociativeBinaryOperatorList : public IParserBase
{
private:
    Operators_t operators;//保存着允许的运算符及其相应的函数, 各种操作符是代码里写好的, 见.cpp文件
    ParserPtr first_elem_parser;
    ParserPtr remaining_elem_parser;

public:
    /** `operators_` - allowed operators and their corresponding functions
      */
    ParserLeftAssociativeBinaryOperatorList(Operators_t operators_, ParserPtr && first_elem_parser_)
        : operators(operators_), first_elem_parser(std::move(first_elem_parser_))
    {
    }

    ParserLeftAssociativeBinaryOperatorList(Operators_t operators_, ParserPtr && first_elem_parser_,
        ParserPtr && remaining_elem_parser_)
        : operators(operators_), first_elem_parser(std::move(first_elem_parser_)),
          remaining_elem_parser(std::move(remaining_elem_parser_))
    {
    }

protected:
    const char * getName() const { return "list, delimited by binary operators"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


/** Expression with an infix operator of arbitrary arity.  中缀表达式, 可以有任意数量个参数
  * For example, a AND b AND c AND d.                      举例1: a AND b AND c AND ....
  *                                                        举例2: a OR b OR c OR ....
  */
class ParserVariableArityOperatorList : public IParserBase
{
private:
    const char * infix;
    const char * function_name;
    ParserPtr elem_parser;

public:
    ParserVariableArityOperatorList(const char * infix_, const char * function_, ParserPtr && elem_parser_)
        : infix(infix_), function_name(function_), elem_parser(std::move(elem_parser_))
    {
    }

protected:
    const char * getName() const { return "list, delimited by operator of variable arity"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


/** An expression with a prefix unary operator.一元前缀表达式
  * Example, NOT x.
  */
class ParserPrefixUnaryOperatorExpression : public IParserBase
{
private:
    Operators_t operators;
    ParserPtr elem_parser;

public:
    /** `operators_` - allowed operators and their corresponding functions
      */
    ParserPrefixUnaryOperatorExpression(Operators_t operators_, ParserPtr && elem_parser_)
        : operators(operators_), elem_parser(std::move(elem_parser_))
    {
    }

protected:
    const char * getName() const { return "expression with prefix unary operator"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserTupleElementExpression : public IParserBase
{
private:
    static const char * operators[];

protected:
    const char * getName() const { return "tuple element expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserArrayElementExpression : public IParserBase
{
private:
    static const char * operators[];

protected:
    const char * getName() const { return "array element expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserUnaryMinusExpression : public IParserBase
{
private:
    static const char * operators[];
    ParserPrefixUnaryOperatorExpression operator_parser {operators, std::make_unique<ParserTupleElementExpression>()};

protected:
    const char * getName() const { return "unary minus expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserMultiplicativeExpression : public IParserBase
{
private:
    static const char * operators[];
    ParserLeftAssociativeBinaryOperatorList operator_parser {operators, std::make_unique<ParserUnaryMinusExpression>()};

protected:
    const char * getName() const { return "multiplicative expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        return operator_parser.parse(pos, node, expected);
    }
};


/// Optional conversion to INTERVAL data type. Example: "INTERVAL x SECOND" parsed as "toIntervalSecond(x)".
class ParserIntervalOperatorExpression : public IParserBase
{
protected:
    ParserMultiplicativeExpression next_parser;

    const char * getName() const { return "INTERVAL operator expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserAdditiveExpression : public IParserBase
{
private:
    static const char * operators[];//类静态常量, .cpp文件中有其具体的内容
    ParserLeftAssociativeBinaryOperatorList operator_parser {operators, std::make_unique<ParserIntervalOperatorExpression>()};

protected:
    const char * getName() const { return "additive expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        return operator_parser.parse(pos, node, expected);
    }
};


class ParserConcatExpression : public IParserBase
{
    ParserVariableArityOperatorList operator_parser {"||", "concat", std::make_unique<ParserAdditiveExpression>()};

protected:
    const char * getName() const { return "string concatenation expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        return operator_parser.parse(pos, node, expected);
    }
};


class ParserBetweenExpression : public IParserBase//BETWEEN AND运算符
{
private:
    ParserConcatExpression elem_parser;

protected:
    const char * getName() const { return "BETWEEN expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserComparisonExpression : public IParserBase
{
private:
    static const char * operators[];//类静态常量, .cpp文件中有其具体的内容
    ParserLeftAssociativeBinaryOperatorList operator_parser {operators, std::make_unique<ParserBetweenExpression>()};

protected:
    const char * getName() const { return "comparison expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        return operator_parser.parse(pos, node, expected);
    }
};


/** Parser for nullity checking with IS (NOT) NULL.
  */
class ParserNullityChecking : public IParserBase//是否为NULL
{
protected:
    const char * getName() const override { return "nullity checking"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserLogicalNotExpression : public IParserBase//逻辑NOT运算符
{
private:
    static const char * operators[];//类静态常量, .cpp文件中有其具体的内容
    ParserPrefixUnaryOperatorExpression operator_parser {operators, std::make_unique<ParserNullityChecking>()};

protected:
    const char * getName() const { return "logical-NOT expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        return operator_parser.parse(pos, node, expected);
    }
};


class ParserLogicalAndExpression : public IParserBase//逻辑AND运算符
{
private:
    ParserVariableArityOperatorList operator_parser {"AND", "and", std::make_unique<ParserLogicalNotExpression>()};

protected:
    const char * getName() const { return "logical-AND expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        return operator_parser.parse(pos, node, expected);
    }
};


class ParserLogicalOrExpression : public IParserBase//逻辑OR运算符
{
private:
    ParserVariableArityOperatorList operator_parser {"OR", "or", std::make_unique<ParserLogicalAndExpression>()};

protected:
    const char * getName() const { return "logical-OR expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        return operator_parser.parse(pos, node, expected);
    }
};


/** An expression with ternary operator.
  * For example, a = 1 ? b + 1 : c * 2.
  */
class ParserTernaryOperatorExpression : public IParserBase
{
private:
    ParserLogicalOrExpression elem_parser;

protected:
    const char * getName() const { return "expression with ternary operator"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserLambdaExpression : public IParserBase//LambdaExpression Lambda表达式
{
private:
    ParserTernaryOperatorExpression elem_parser;//TernaryOperatorExpression 三元表达式

protected:
    const char * getName() const { return "lambda expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


using ParserExpression = ParserLambdaExpression;


class ParserExpressionWithOptionalAlias : public IParserBase
{
public:
    ParserExpressionWithOptionalAlias(bool allow_alias_without_as_keyword/*是否允许指定别名时不使用AS关键字*/, bool prefer_alias_to_column_name_ = false/*是否更喜欢别名而不是列名*/);
protected:
    ParserPtr impl;

    const char * getName() const { return "expression with optional alias"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {
        return impl->parse(pos, node, expected);
    }
};


/** A comma-separated list of expressions, probably empty. */
class ParserExpressionList : public IParserBase
{
public:
    ParserExpressionList(bool allow_alias_without_as_keyword_, bool prefer_alias_to_column_name_ = false)
        : allow_alias_without_as_keyword(allow_alias_without_as_keyword_), prefer_alias_to_column_name(prefer_alias_to_column_name_) {}

protected:
    bool allow_alias_without_as_keyword;
    bool prefer_alias_to_column_name;

    const char * getName() const { return "list of expressions"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserNotEmptyExpressionList : public IParserBase
{
public:
    ParserNotEmptyExpressionList(bool allow_alias_without_as_keyword, bool prefer_alias_to_column_name = false)
        : nested_parser(allow_alias_without_as_keyword, prefer_alias_to_column_name) {}
private:
    ParserExpressionList nested_parser;
protected:
    const char * getName() const { return "not empty list of expressions"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserOrderByExpressionList : public IParserBase
{
protected:
    const char * getName() const { return "order by expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


}
