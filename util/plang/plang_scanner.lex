%{
#include "util/plang/plang_scanner.h"
#undef YY_DECL
// ORI: YY_DECL allows us to choose the name we want for the lex function,
//      and also to change the parameters to those that the parser will send
//      (currently I am not using these in lexer code, but this may change
//       to faciliate better diagnostics)
#define YY_DECL int plang::Scanner::parser_lex(plang::Parser::semantic_type * const lval, \
                                               plang::Parser::location_type *location)
// ORI: YY_USER_ACTION runs before every flex rule. This allows us to update our location variable.
//      note that this doesn't interact well with yymore() (as it will be called several times
//      during token construction)
#define YY_USER_ACTION loc_->step(); loc_->columns(yyleng);
%}

%option yyclass="plang::Scanner"
%option noyywrap
%option c++

%%
%{
   using TOKEN = plang::Parser::token::yytokentype;
%}
[ \t\n]+                            // skip white space chars.
-?[0-9]+                         return TOKEN::NUMBER;
-?[0-9]*"."[0-9]+                return TOKEN::NUMBER;
(?i:"and")|"&&"                  return TOKEN::AND_OP;
(?i:"or")|"||"                 return TOKEN::OR_OP;
"<="                           return TOKEN::LE_OP;
">="                           return TOKEN::GE_OP;
(?i:"not")                     return TOKEN::NOT_OP;
"!="                           return TOKEN::NE_OP;
(?i:"rlike")|(?i:"regexp")     return TOKEN::RLIKE_OP;
(?i:"true")                    {
                                 yytext[0] = '1';
                                 yytext[1] = '\0';
                                 yyleng = 1;
                                 return TOKEN::NUMBER;
                               }
(?i:"false")                   {
                                 yytext[0] = '0';
                                 yytext[1] = '\0';
                                 yyleng = 1;
                                 return TOKEN::NUMBER;
                               }
(?i:"def")                     return TOKEN::DEF_TOK;
[[:alpha:]_][[:alnum:]_.]*     return TOKEN::IDENTIFIER;
\"([^"]|\\.)*\"|\'([^']|\\.)*\' {
                  memmove(yytext, yytext + 1, yyleng);
                  yyleng -= 2;
                  yytext[yyleng] = '\0';
                  return TOKEN::STRING;
                }
[^\"]           return yytext[0];
.               yyterminate();
%{
#undef SIMPLE
%}
