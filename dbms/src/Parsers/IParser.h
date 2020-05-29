#pragma once

#include <set>
#include <memory>

#include <Core/Defines.h>
#include <Core/Types.h>
#include <Parsers/IAST.h>
#include <Parsers/TokenIterator.h>


namespace DB {

/** Collects variants, how parser could proceed further at rightmost position.
  */
    struct Expected {
        const char *max_parsed_pos = nullptr;
        std::set<const char *> variants;

        /// 'description' should be statically allocated string.
        void add(const char *current_pos, const char *description) {
            if (!max_parsed_pos || current_pos > max_parsed_pos) {
                variants.clear();
                max_parsed_pos = current_pos;
            }

            if (!max_parsed_pos || current_pos >= max_parsed_pos)
                variants.insert(description);
        }

        void add(TokenIterator it, const char *description) {
            add(it->begin, description);
        }
    };


/** Interface for parser classes
  */
    class IParser {
    public:
        using Pos = TokenIterator;

        /** Get the text of this parser parses. */
        virtual const char *getName() const = 0;

        /** Parse piece of text from position `pos`, but not beyond end of line (`end` - position after end of line),
          * move pointer `pos` to the maximum position to which it was possible to parse,
          * in case of success return `true` and the result in `node` if it is needed, otherwise false,
          * in `expected` write what was expected in the maximum position,
          *  to which it was possible to parse if parsing was unsuccessful,
          *  or what this parser parse if parsing was successful.
          * The string to which the [begin, end) range is included may be not 0-terminated.
          */
        /** 从位置pos处开始解析一段内容, 内容不要超出行尾(即 内容结尾处在[end, 行尾之后的位置]之间),
          * 将指针pos移至可以解析的最大位置.
          * 如果解析成功, 则返回true, 如果有必要, 可将结果保存在node中返回; 否则解析失败, 返回false;

          * 将 期望在maximum position处解析的内容 保存到expected中, 如果解析不成功, 则可以解析到最大位置; 如果解析成功, 则可以解析到该解析器解析的内容。
          *
          * 注意: [begin, end）范围的字符串不能以0结尾
          */
        virtual bool parse(Pos &pos, ASTPtr &node, Expected &expected) = 0;

        //ignore方法的作用应该是检测当前解析的token是否是关键字, 如果是关键字则返回true, 如果不是关键字则返回false
        bool ignore(Pos &pos, Expected &expected) {
            ASTPtr ignore_node;
            return parse(pos, ignore_node, expected);
        }

        bool ignore(Pos &pos) {
            Expected expected;
            return ignore(pos, expected);
        }

        /** The same, but do not move the position and do not write the result to node.
          */
        bool check(Pos &pos, Expected &expected) {
            Pos begin = pos;
            ASTPtr node;
            if (!parse(pos, node, expected)) {
                pos = begin;
                return false;
            } else
                return true;
        }

        /* The same, but never move the position and do not write the result to node.
         */
        bool check_without_moving(Pos pos, Expected &expected) {
            ASTPtr node;
            return parse(pos, node, expected);
        }

        virtual ~IParser() {}
    };

    using ParserPtr = std::unique_ptr<IParser>;

}
