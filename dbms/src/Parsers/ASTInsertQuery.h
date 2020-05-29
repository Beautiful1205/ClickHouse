#pragma once

#include <Parsers/IAST.h>


namespace DB {


/** INSERT query
  */
    class ASTInsertQuery : public IAST {
    public:
        String database;
        String table;
        ASTPtr columns;
        String format;
        ASTPtr select;
        ASTPtr table_function;
        ASTPtr settings_ast;

        /// Set to true if the data should only be inserted into attached views
        // no_destination = true, 则数据不需要insert到表中 且 只需要insert到视图中
        // no_destination = false, 则数据需要insert到表中 且 需要insert到视图中
        //所以是先向view中写数据(如果有view的话), 再向表中写数据么？？？
        bool no_destination = false;

        /// Data to insert
        const char *data = nullptr;
        const char *end = nullptr;

        /// Query has additional data, which will be sent later
        bool has_tail = false;

        /** Get the text that identifies this element. */
        String getID(char delim) const override { return "InsertQuery" + (delim + database) + delim + table; }

        ASTPtr clone() const override {
            auto res = std::make_shared<ASTInsertQuery>(*this);
            res->children.clear();

            if (columns) {
                res->columns = columns->clone();
                res->children.push_back(res->columns);
            }
            if (select) {
                res->select = select->clone();
                res->children.push_back(res->select);
            }
            if (table_function) {
                res->table_function = table_function->clone();
                res->children.push_back(res->table_function);
            }
            if (settings_ast) {
                res->settings_ast = settings_ast->clone();
                res->children.push_back(res->settings_ast);
            }

            return res;
        }

    protected:
        void formatImpl(const FormatSettings &settings, FormatState &state, FormatStateStacked frame) const override;
    };

}
