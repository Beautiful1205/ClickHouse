#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

/** Single SELECT query or multiple SELECT queries with UNION ALL.
  * Only UNION ALL is possible. No UNION DISTINCT or plain UNION.
  *
  * 单独的SELECT语句 或者 多个用UNION ALL连接的SELECT语句
  * SELECT语句之间只能用UNION ALL连接, 不能用UNION DISTINCT或是UNION
  */
class ASTSelectWithUnionQuery : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return "SelectWithUnionQuery"; }

    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr list_of_selects;
};

}
