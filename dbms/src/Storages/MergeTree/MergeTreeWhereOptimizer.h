#pragma once

#include <memory>
#include <unordered_map>
#include <set>
#include <boost/noncopyable.hpp>
#include <Core/Block.h>
#include <Storages/SelectQueryInfo.h>


namespace Poco { class Logger; }

namespace DB {

    class ASTSelectQuery;

    class ASTFunction;

    class MergeTreeData;

/** Identifies WHERE expressions that can be placed in PREWHERE by calculating respective
 *  sizes of columns used in particular expression and identifying "good" conditions of
 *  form "column_name = constant", where "constant" is outside some `threshold` specified in advance.
 *
 *  If there are "good" conditions present in WHERE, the one with minimal summary column size is transferred to PREWHERE.
 *  Otherwise any condition with minimal summary column size can be transferred to PREWHERE.
 */
    class MergeTreeWhereOptimizer : private boost::noncopyable {
    public:
        MergeTreeWhereOptimizer(
                SelectQueryInfo &query_info,
                const Context &context,
                const MergeTreeData &data,
                const Names &queried_column_names,
                Poco::Logger *log);

    private:
        void optimize(ASTSelectQuery &select) const;

        struct Condition {
            ASTPtr node;
            UInt64 columns_size = 0;
            NameSet identifiers;
            bool viable = false;
            bool good = false;

            auto tuple() const {
                return std::make_tuple(!viable, !good, columns_size);//注意这里定的是!viable, !good, 所以前面的方法中要取std::min_element
            }

            /// Is condition a better candidate for moving to PREWHERE?
            // 按字典顺序比较 tuple 中的值, 先比较tuple中的第一个元素, 相同的话比较第二个......
            //false --- 0 ; true --- 1;
            bool operator<(const Condition &rhs) const {
                return tuple() < rhs.tuple();  //如果有两个元素根据前面的规则不能判断大小, 则根据加入到list中的顺序比较, 后加入的较小
            }
        };

        using Conditions = std::list<Condition>;

        void analyzeImpl(Conditions &res, const ASTPtr &node) const;

        /// Transform conjunctions chain in WHERE expression to Conditions list.
        Conditions analyze(const ASTPtr &expression) const;

        /// Transform Conditions list to WHERE or PREWHERE expression.
        ASTPtr reconstruct(const Conditions &conditions) const;

        void calculateColumnSizes(const MergeTreeData &data, const Names &column_names);

        void optimizeConjunction(ASTSelectQuery &select, ASTFunction *const fun) const;

        void optimizeArbitrary(ASTSelectQuery &select) const;

        UInt64 getIdentifiersColumnSize(const NameSet &identifiers) const;

        bool isConditionGood(const ASTPtr &condition) const;

        bool hasPrimaryKeyAtoms(const ASTPtr &ast) const;

        bool isPrimaryKeyAtom(const ASTPtr &ast) const;

        bool isConstant(const ASTPtr &expr) const;

        bool isSubsetOfTableColumns(const NameSet &identifiers) const;

        /** ARRAY JOIN'ed columns as well as arrayJoin() result cannot be used in PREWHERE, therefore expressions
          *    containing said columns should not be moved to PREWHERE at all.
          *    We assume all AS aliases have been expanded prior to using this class
          *
          * Also, disallow moving expressions with GLOBAL [NOT] IN.
          */
        bool cannotBeMoved(const ASTPtr &ptr) const;

        void determineArrayJoinedNames(ASTSelectQuery &select);

        using StringSet = std::unordered_set<std::string>;

        String first_primary_key_column;
        const StringSet table_columns;
        const Names queried_columns;
        const Block block_with_constants;
        Poco::Logger *log;
        std::unordered_map<std::string, UInt64> column_sizes;
        UInt64 total_size_of_queried_columns = 0;
        NameSet array_joined_names;
    };


}
