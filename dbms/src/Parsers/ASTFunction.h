#pragma once

#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTExpressionList.h>


namespace DB {

/** AST for function application or operator.
  */
    class ASTFunction : public ASTWithAlias {
    public:
        String name;    //关系操作符, 可能的取值包括"equals"、"greater"、"and"、"or"、"not"等等
        ASTPtr arguments;  //常量, 应该是表中的某个列名
        /// parameters - for parametric aggregate function. Example: quantile(0.9)(x) - what in first parens are 'parameters'.
        ASTPtr parameters;  //变量, 应该是表中的某个列的具体取值

    public:
        /** Get text identifying the AST node. */
        String getID(char delim) const override;

        ASTPtr clone() const override;

    protected:
        void formatImplWithoutAlias(const FormatSettings &settings, FormatState &state,
                                    FormatStateStacked frame) const override;

        void appendColumnNameImpl(WriteBuffer &ostr) const override;
    };


    template<typename... Args>
    std::shared_ptr<ASTFunction> makeASTFunction(const String &name, Args &&... args) {
        const auto function = std::make_shared<ASTFunction>();

        function->name = name;
        function->arguments = std::make_shared<ASTExpressionList>();
        function->children.push_back(function->arguments);

        function->arguments->children = {std::forward<Args>(args)...};

        return function;
    }

}
