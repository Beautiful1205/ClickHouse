#pragma once

#include <Common/typeid_cast.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

    class Context;

    class ASTSubquery;

    class ASTFunction;

    struct ASTTableExpression;

    /** Replace subqueries that return exactly one row ("scalar" subqueries) to the corresponding constants.
      * If the subquery returns more than one column, it is replaced by a tuple of constants.
      * 使用常量替换标量子查询. 如果子查询返回多列, 则使用常量元组替换子查询.
      *
      * Features
      *
      * A replacement occurs during query analysis, and not during the main runtime.      替换发生在查询分析期间, 而不是在主运行时期间
      * This means that the progress indicator will not work during the execution of these requests,
      *  and also such queries can not be aborted.                          进度指示器在执行这些请求期间将不起作用, 而且此类查询也不能中止
      *
      * But the query result can be used for the index in the table.     查询结果可以用于表中的索引
      *
      * Scalar subqueries are executed on the request-initializer server.           标量子查询在初始化request的server上执行
      * The request is sent to remote servers with already substituted constants.   将request中的标量子查询替换成常量后, 再将request发送到远程server上执行
      */
    //标量子查询在初始化request的server上执行. 将request中的标量子查询替换成常量后, 再将request发送到远程server上执行
    class ExecuteScalarSubqueriesMatcher
    {
    public:
        using Visitor = InDepthNodeVisitor<ExecuteScalarSubqueriesMatcher, true>;

        struct Data
        {
            const Context &context;
            size_t subquery_depth;
        };

        static bool needChildVisit(ASTPtr &node, const ASTPtr &);

        static void visit(ASTPtr &ast, Data &data);

    private:
        static void visit(const ASTSubquery &subquery, ASTPtr &ast, Data &data);

        static void visit(const ASTFunction &func, ASTPtr &ast, Data &data);
    };

    using ExecuteScalarSubqueriesVisitor = ExecuteScalarSubqueriesMatcher::Visitor;

}
