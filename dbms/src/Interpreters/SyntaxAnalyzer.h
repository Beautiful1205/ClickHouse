#pragma once

#include <Interpreters/Aliases.h>
#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Storages/IStorage_fwd.h>

namespace DB {

    NameSet removeDuplicateColumns(NamesAndTypesList &columns);

    struct SyntaxAnalyzerResult {
        StoragePtr storage;//存储引擎

        NamesAndTypesList source_columns;//表中所有的列名和对应的类型

        Aliases aliases;//别名

        /// Which column is needed to be ARRAY-JOIN'ed to get the specified.
        /// For example, for `SELECT s.v ... ARRAY JOIN a AS s` will get "s.v" -> "a.v".
        NameToNameMap array_join_result_to_source;//ARRAY JOIN操作后, PROJECT时候的列名映射关系, 如"s.v" -> "a.v"

        /// For the ARRAY JOIN section, mapping from the alias to the full column name.
        /// For example, for `ARRAY JOIN [1,2] AS b` "b" -> "array(1,2)" will enter here.
        /// Note: not used further.
        NameToNameMap array_join_alias_to_name;//ARRAY JOIN操作后的别名映射, 如"b" -> "array(1,2)"

        /// The backward mapping for array_join_alias_to_name.
        /// Note: not used further.
        NameToNameMap array_join_name_to_alias;//ARRAY JOIN操作后的别名映射, 如"array(1,2)" -> "b"

        AnalyzedJoin analyzed_join;//JOIN的分析结果

        /// Predicate optimizer overrides the sub queries
        bool rewrite_subqueries = false;//子查询是否会被优化
    };

    using SyntaxAnalyzerResultPtr = std::shared_ptr<const SyntaxAnalyzerResult>;

/// AST syntax analysis.                                             AST语法分析. 优化AST树并收集信息以进行进一步的表达式分析
/// Optimises AST tree and collect information for further expression analysis.
/// Result AST has the following invariants:
///  * all aliases are substituted                                               别名替换
///  * qualified names are translated                                            限定名转换
///  * scalar subqueries are executed replaced with constants                    标量子查询用常量替换
///  * unneeded columns are removed from SELECT clause                           从SELECT子句中删除不需要的列
///  * duplicated columns are removed from ORDER BY, LIMIT BY, USING(...).       删除重复列
/// Motivation:
///  * group most of the AST-changing operations in single place
///  * avoid AST rewriting in ExpressionAnalyzer
///  * decompose ExpressionAnalyzer
    class SyntaxAnalyzer {
    public:
        SyntaxAnalyzer(const Context &context_, const SelectQueryOptions &select_options = {})
                : context(context_), subquery_depth(select_options.subquery_depth),
                  remove_duplicates(select_options.remove_duplicates) {}

        SyntaxAnalyzerResultPtr analyze(
                ASTPtr &query,
                const NamesAndTypesList &source_columns_,
                const Names &required_result_columns = {},
                StoragePtr storage = {}) const;

    private:
        const Context &context;
        size_t subquery_depth;
        bool remove_duplicates;
    };

}
