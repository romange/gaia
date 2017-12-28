// This code is based on http://www.jonathanbeard.io/tutorials/FlexBisonC++

#ifndef _PLANG_SCANNER_H
#define _PLANG_SCANNER_H

#if ! defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

namespace plang {
class Scanner;
class Driver;
}
#include "util/plang/plang.h"
#include "util/plang/plang_parser.hh"

namespace plang {
class Scanner : public yyFlexLexer {
 public:
  Scanner(std::istream *in) : yyFlexLexer(in) {
    loc_.reset(new plang::Parser::location_type());
  }
  
  int lex() {
    Parser::semantic_type lval;
    Parser::location_type location;
    return parser_lex(&lval, &location);
  }
  
  int parser_lex( Parser::semantic_type * const lval,
                  Parser::location_type *location);
  
  std::string matched() { return yytext; }

  Parser::location_type *loc() { return &*loc_; }

 private:
  std::unique_ptr<plang::Parser::location_type> loc_;
};    
}; 

#endif
