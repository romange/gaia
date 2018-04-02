%defines
%define api.namespace {plang}
%define api.value.type variant
%define parser_class_name {Parser}

%token NUMBER
%token IDENTIFIER STRING DEF_TOK NOT_OP

%type <Expr* > bool_expr scalar_expr comparison_predicate func_ref
%type <std::string> identifier
%type <ArgList > arg_list


                                 // lowest precedence
%left OR_OP
%left AND_OP
%left LE_OP GE_OP '<' '>'
%left   '=' NE_OP RLIKE_OP
%right NOT_OP
                                // highest precedence

%locations

%parse-param { Scanner  *scanner  }
%parse-param { std::unique_ptr<Expr> *output }

%code top {
#include "util/plang/plang.h"
}

%code requires {
namespace plang {
class Scanner; // ORI: Parser & Scanner headers are mutually dependent, which forces us to do this
}
}

%code{
#include "base/logging.h"
#include "strings/numbers.h"
#include "util/plang/plang_scanner.h"

#undef yylex
#define yylex scanner->parser_lex

void plang::Parser::error(plang::location const &, std::string const &m) {
  // ORI: This is not the best implementation, I just copied what was previously done
  //      with bisonc++ and flexc++
  std::cerr << m << '\n';
}
}

%%

input:
    // empty
  | bool_expr
  {
    output->reset($1);
  }
;

bool_expr:
    bool_expr AND_OP bool_expr
    {
      // std::cout << $1 << " AND " << $3 << '\n';
      $$ = new BinOp(BinOp::AND, $1, $3);
    }
|   bool_expr OR_OP bool_expr
    {
      // std::cout << $1 << " AND " << $3 << '\n';
      $$ = new BinOp(BinOp::OR, $1, $3);
    }
|   comparison_predicate
    {
      $$ = $1; // for variants, this must be explicit
    }
|  '(' bool_expr ')'
    {
      $$ = $2;
    }
| DEF_TOK '(' identifier ')'
  {
    $$ = new IsDefFun($3);
  }
| NOT_OP bool_expr
  {
     $$ = new BinOp(BinOp::NOT, $2, nullptr);
  }
;

comparison_predicate :
    scalar_expr '=' scalar_expr
    {
       // std::cout << $1 << " EQ " << $3 << '\n';
       $$ = new BinOp(BinOp::EQ, $1, $3);
    }
|   scalar_expr NE_OP scalar_expr
    {
       $$ = new BinOp(BinOp::NOT, new BinOp(BinOp::EQ, $1, $3), nullptr);
    }
|   scalar_expr RLIKE_OP scalar_expr
    {
      $$ = new BinOp(BinOp::RLIKE, $1, $3);
    }
|  scalar_expr '<' scalar_expr
   {
     $$ = new BinOp(BinOp::LT, $1, $3);
   }
|  scalar_expr LE_OP scalar_expr
   {
     $$ = new BinOp(BinOp::LE, $1, $3);
   }
|  scalar_expr GE_OP scalar_expr
   {
     $$ = new BinOp(BinOp::LE, $3, $1);
   }
|  scalar_expr '>' scalar_expr
   {
     $$ = new BinOp(BinOp::LT, $3, $1);
   }
;


scalar_expr:
   identifier
   {
      $$ = new StringTerm($1, StringTerm::VARIABLE);
   }
 | func_ref
   {
      $$ = $1; // for variants, this must be explicit
   }
 | NUMBER
   {
      // std::cout << " number " << d_scanner.matched() << '\n';
      int64 tmp;
      uint64 tmp2;
      double tmp3;
      if (safe_strto64(scanner->matched(), &tmp)) {
        $$ = new NumericLiteral(NumericLiteral::Signed(tmp));
      } else if (safe_strtou64(scanner->matched(), &tmp2)) {
        $$ = new NumericLiteral(NumericLiteral::Unsigned(tmp2));
      } else {
        CHECK(safe_strtod(scanner->matched(), &tmp3)) << scanner->matched();
        $$ = new NumericLiteral(NumericLiteral::Double(tmp3));
      }
   }
 | STRING
   {
      // std::cout << " STRING " << d_scanner.matched() << '\n';
      $$ = new StringTerm(scanner->matched(), StringTerm::CONST);
   }
|  '(' scalar_expr ')'
   {
      $$ = $2;
   }
;

identifier : IDENTIFIER
 {
    // std::cout << " IDENTIFIER " << d_scanner.matched() << '\n';
    $$ = scanner->matched();
 }
 ;

func_ref:
   identifier '(' arg_list ')'
   {
     // std::cout << " function " << $1 << '\n';
     $$ = new FunctionTerm($1, std::move($3));
   }
;


arg_list : scalar_expr
    {
      $$ = ArgList{$1};
    }
|   arg_list ',' scalar_expr
    {
      $1.push_back($3);
      $$ = move($1);
    }
;
