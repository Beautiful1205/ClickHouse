#pragma once

#include <Parsers/IParser.h>


namespace DB {

/** Base class for most parsers
  */
    class IParserBase : public IParser {
    public:
        bool parse(Pos &pos, ASTPtr &node, Expected &expected);

    protected:
        //继承IParserBase的类中有具体的parseImpl的实现方法
        virtual bool parseImpl(Pos &pos, ASTPtr &node, Expected &expected) = 0;
    };

}
