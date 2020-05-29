#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

#include <memory>
#include <string>


namespace DB
{

class Context;

/** Interface for table functions. 表函数接口
  *
  * 这里的表函数和其他函数无关. 表函数和[db.]Table的含义一样, 表示数据的来源.
  * 表函数返回一个用于执行查询的临时StoragePtr对象
  * Table functions are not relevant to other functions.
  * The table function can be specified in the FROM section instead of the [db.]Table
  * The table function returns a temporary StoragePtr object that is used to execute the query.
  *
  * Example:
  * SELECT count() FROM remote('example01-01-1', merge, hits)
  * - go to `example01-01-1`, in `merge` database, `hits` table.
  */

class ITableFunction
{
public:
    /// Get the main function name.
    virtual std::string getName() const = 0;

    /// Create storage according to the query.
    StoragePtr execute(const ASTPtr & ast_function, const Context & context) const;

    virtual ~ITableFunction() {}

private:
    virtual StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context) const = 0;
};

using TableFunctionPtr = std::shared_ptr<ITableFunction>;


}
