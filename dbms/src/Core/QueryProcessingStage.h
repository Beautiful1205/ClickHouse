#pragma once

#include <Core/Types.h>


namespace DB
{

/// Up to what stage the SELECT query is executed or needs to be executed.
namespace QueryProcessingStage
{
    /// Numbers matter - the later stage has a larger number.
    enum Enum
    {
        //仅读取/已经读取查询中指定的列
        FetchColumns       = 0,    /// Only read/have been read the columns specified in the query.
        //分布式查询中, 各个server的结果可以进行聚合的中间状态
        WithMergeableState = 1,    /// Until the stage where the results of processing on different servers can be combined.
        //完成阶段
        Complete           = 2,    /// Completely.
    };

    inline const char * toString(UInt64 stage)
    {
        static const char * data[] = { "FetchColumns", "WithMergeableState", "Complete" };
        return stage < 3
            ? data[stage]
            : "Unknown stage";
    }
}

}
