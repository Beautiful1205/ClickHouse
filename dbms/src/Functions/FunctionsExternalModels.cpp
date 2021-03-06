#include <Functions/FunctionsExternalModels.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExternalModels.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <ext/range.h>
#include <string>
#include <memory>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/assert_cast.h>


namespace DB
{

FunctionPtr FunctionModelEvaluate::create(const Context & context)
{
    return std::make_shared<FunctionModelEvaluate>(context.getExternalModels());
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
}

DataTypePtr FunctionModelEvaluate::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() < 2)
        throw Exception("Function " + getName() + " expects at least 2 arguments",
                        ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);

    if (!isString(arguments[0].type))
        throw Exception("Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName()
                        + ", expected a string.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    const auto name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
    if (!name_col)
        throw Exception("First argument of function " + getName() + " must be a constant string",
                        ErrorCodes::ILLEGAL_COLUMN);

    bool has_nullable = false;
    for (size_t i = 1; i < arguments.size(); ++i)
        has_nullable = has_nullable || arguments[i].type->isNullable();

    auto model = models.getModel(name_col->getValue<String>());
    auto type = model->getReturnType();

    if (has_nullable)
    {
        if (auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
        {
            auto elements = tuple->getElements();
            for (auto & element : elements)
                element = makeNullable(element);

            type = std::make_shared<DataTypeTuple>(elements);
        }
        else
            type = makeNullable(type);
    }

    return type;
}

void FunctionModelEvaluate::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const auto name_col = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
    if (!name_col)
        throw Exception("First argument of function " + getName() + " must be a constant string",
                        ErrorCodes::ILLEGAL_COLUMN);

    auto model = models.getModel(name_col->getValue<String>());

    ColumnRawPtrs columns;
    Columns materialized_columns;
    ColumnPtr null_map;

    columns.reserve(arguments.size());
    for (auto arg : ext::range(1, arguments.size()))
    {
        auto & column = block.getByPosition(arguments[arg]).column;
        columns.push_back(column.get());
        if (auto full_column = column->convertToFullColumnIfConst())
        {
            materialized_columns.push_back(full_column);
            columns.back() = full_column.get();
        }
        if (auto * col_nullable = checkAndGetColumn<ColumnNullable>(*columns.back()))
        {
            if (!null_map)
                null_map = col_nullable->getNullMapColumnPtr();
            else
            {
                auto mut_null_map = (*std::move(null_map)).mutate();

                NullMap & result_null_map = assert_cast<ColumnUInt8 &>(*mut_null_map).getData();
                const NullMap & src_null_map = col_nullable->getNullMapColumn().getData();

                for (size_t i = 0, size = result_null_map.size(); i < size; ++i)
                    if (src_null_map[i])
                        result_null_map[i] = 1;

                null_map = std::move(mut_null_map);
            }

            columns.back() = &col_nullable->getNestedColumn();
        }
    }

    auto res = model->evaluate(columns);

    if (null_map)
    {
        if (auto * tuple = typeid_cast<const ColumnTuple *>(res.get()))
        {
            auto nested = tuple->getColumns();
            for (auto & col : nested)
                col = ColumnNullable::create(col, null_map);

            res = ColumnTuple::create(nested);
        }
        else
            res = ColumnNullable::create(res, null_map);
    }

    block.getByPosition(result).column = res;
}

void registerFunctionsExternalModels(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModelEvaluate>();
}

}
