// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/plang/plang.h"

#include "base/logging.h"
#include "util/plang/addressbook.pb.h"
#include "util/plang/plang_scanner.h"
#include "util/plang/plang_parser.hh"
#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace plang {

using namespace tutorial;

class PlangTest : public testing::Test {
protected:
  void scan(const std::string& val) {
    istr_.clear();
    istr_.str(val);
    scanner_.reset(new Scanner(&istr_));
  }

  int parse(const std::string& val) {
    istr_.clear();
    istr_.str(val);
    scanner_.reset(new Scanner(&istr_));
    parser_.reset(new Parser(scanner_.get(), &res_val_));
    return parser_->parse();
  }

  bool eval(const ::google::protobuf::Message &msg) {
    CHECK(res_val_);
    return EvaluateBoolExpr(*res_val_, msg);
  }

  const Expr& get_parsed() {
    return *res_val_.get();
  }

  std::istringstream istr_;
  std::unique_ptr<Scanner> scanner_;
  std::unique_ptr<Parser> parser_;
  std::unique_ptr<Expr> res_val_;
};

const char kNameVal[] = "Roman";
const char kBankVal[] = "hapoalim";

TEST_F(PlangTest, Basic) {
  Person person;
  person.set_name(kNameVal);
  person.mutable_account()->set_bank_name(kBankVal);
  person.set_id(5);

  StringTerm term("name", StringTerm::VARIABLE);
  ExprValue res;
  auto res_setter = [&res](const ExprValue& val) { res = val; return false;};
  term.eval(person, res_setter);
  ASSERT_EQ(ExprValue::CPPTYPE_STRING, res.type);
  EXPECT_EQ(kNameVal, res.val.str);

  {
    StringTerm term2("account.bank_name", StringTerm::VARIABLE);
    term2.eval(person, res_setter);

    ASSERT_EQ(ExprValue::CPPTYPE_STRING, res.type);
    EXPECT_EQ(kBankVal, res.val.str);
  }

  {
    StringTerm term("id", StringTerm::VARIABLE);
    term.eval(person, res_setter);

    ASSERT_EQ(ExprValue::CPPTYPE_INT64, res.type);
    EXPECT_EQ(5, res.val.int_val);
  }
}

TEST_F(PlangTest, Op) {
  Person person;
  person.set_name(kNameVal);
  person.set_id(6);
  ExprValue res;
  auto res_setter = [&res](const ExprValue& val) { res = val; return false;};
  {
    BinOp op(BinOp::EQ, new StringTerm(kNameVal, StringTerm::CONST),
             new StringTerm("name", StringTerm::VARIABLE));
    op.eval(person, res_setter);
    ASSERT_EQ(ExprValue::CPPTYPE_BOOL, res.type);
    EXPECT_TRUE(res.val.int_val);
  }

  {
    BinOp op(BinOp::EQ, new StringTerm("Anna", StringTerm::CONST),
             new StringTerm("name", StringTerm::VARIABLE));
    op.eval(person, res_setter);
    ASSERT_EQ(ExprValue::CPPTYPE_BOOL, res.type);
    EXPECT_FALSE(res.val.int_val);
  }

   {
    BinOp op(BinOp::EQ, new NumericLiteral(NumericLiteral::Signed(6)),
             new StringTerm("id", StringTerm::VARIABLE));
    op.eval(person, res_setter);
    ASSERT_EQ(ExprValue::CPPTYPE_BOOL, res.type);
    EXPECT_TRUE(res.val.int_val);
  }
}

TEST_F(PlangTest, Scanner) {
  using TOKEN = plang::Parser::token::yytokentype;
  scan("ABCD 1234 -2785 a123+6");
  int token = scanner_->lex();
  EXPECT_EQ(TOKEN::IDENTIFIER, token);
  EXPECT_EQ("ABCD", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(TOKEN::NUMBER, token);
  EXPECT_EQ("1234", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(TOKEN::NUMBER, token);
  EXPECT_EQ("-2785", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(TOKEN::IDENTIFIER, token);
  EXPECT_EQ("a123", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(int('+'), token);
  EXPECT_EQ("+", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(TOKEN::NUMBER, token);
  EXPECT_EQ("6", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(0, token);

  scan("a=b");
  token = scanner_->lex();
  EXPECT_EQ(TOKEN::IDENTIFIER, token);
  token = scanner_->lex();
  EXPECT_EQ(int('='), token);
  token = scanner_->lex();
  EXPECT_EQ(TOKEN::IDENTIFIER, token);
}

TEST_F(PlangTest, ScanString) {
  using TOKEN = plang::Parser::token::yytokentype;
  scan("\"Foo\"Bar\"End");
  int token = scanner_->lex();
  EXPECT_EQ(TOKEN::STRING, token);
  EXPECT_EQ("Foo", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(TOKEN::IDENTIFIER, token);
  EXPECT_EQ("Bar", scanner_->matched());
  token = scanner_->lex();
  EXPECT_EQ(0, token);
  EXPECT_EQ("End", istr_.str().substr(scanner_->loc()->begin.column));  // Unmatched rule.

  const char kVar[] = "field_one.field_two";
  scan(kVar);
  token = scanner_->lex();
  EXPECT_EQ(TOKEN::IDENTIFIER, token);
  EXPECT_EQ(kVar, scanner_->matched());
}

TEST_F(PlangTest, Parse) {
  Person person;
  person.set_name("Roman");
  person.set_id(6);
  parse("\"Foo\" = \"Bar\"");
  EXPECT_TRUE(res_val_ != nullptr);
  EXPECT_FALSE(eval(person));

  parse("name = \"Bar\"");
  EXPECT_FALSE(eval(person));

  parse("name = \"Roman\"");
  EXPECT_TRUE(eval(person));

  parse("name RliKE \".*man\"");
  EXPECT_TRUE(eval(person));

  parse("id=6");
  EXPECT_TRUE(eval(person));

  parse("(id=6) AND name=\"Roman\"");
  EXPECT_TRUE(eval(person));

  parse("(id=6) or name=\"Foo\"");
  EXPECT_TRUE(eval(person));

  person.set_id(5);
  EXPECT_FALSE(eval(person));

  person.set_name("Foo");
  EXPECT_TRUE(eval(person));

  person.set_id(-1089);

  parse("id=-1089");
  EXPECT_TRUE(eval(person));
}

TEST_F(PlangTest, Comparison) {
  Person person;
  person.set_id(6);
  person.set_dval(5.5);
  parse("id < 7");
  ASSERT_TRUE(res_val_ != nullptr);
  EXPECT_TRUE(eval(person));
  parse("id > 5");
  EXPECT_TRUE(eval(person));
  parse("id > 6");
  EXPECT_FALSE(eval(person));
  parse("not id > 6");
  EXPECT_TRUE(eval(person));
  parse("id != 5");
  EXPECT_TRUE(eval(person));
  parse("id > 5.9");
  EXPECT_TRUE(eval(person));

  parse("id < 5.9");
  EXPECT_FALSE(eval(person));
  parse("id > 6.1");
  EXPECT_FALSE(eval(person));
  parse("id < 6.1");
  EXPECT_TRUE(eval(person));
  parse("dval > 5.4");
  EXPECT_TRUE(eval(person));
  parse("dval < 5.6");
  EXPECT_TRUE(eval(person));
  parse("dval < 5.4");
  EXPECT_FALSE(eval(person));
  parse("dval > 5.6");
  EXPECT_FALSE(eval(person));
}

TEST_F(PlangTest, Function) {
  Person person;
  person.set_id(6);
  /*parse("id(7,8) = 5");
  ASSERT_TRUE(res_val_ != nullptr);

  parse("def(id)");
  ASSERT_TRUE(res_val_ != nullptr);
  EXPECT_TRUE(eval(person));

  parse("def(phone)");
  EXPECT_FALSE(eval(person));

  parse("def(account.bank_name)");
  EXPECT_FALSE(eval(person));*/

  parse("def(account.address.street)");
  EXPECT_FALSE(eval(person));
}

TEST_F(PlangTest, Enum) {
  Person::PhoneNumber phone;
  phone.set_type(Person::HOME);
  parse("type = 'HOME'");
  EXPECT_TRUE(eval(phone));

  parse("type = 1");
  EXPECT_TRUE(eval(phone));

  parse("type = 'MOBILE'");
  EXPECT_FALSE(eval(phone));
}

TEST_F(PlangTest, RepeatedMsgs) {
  Person person;
  person.add_phone()->set_number("1");
  person.add_phone()->set_number("2");
  person.add_phone()->set_number("3");
  parse("phone.number = '1'");
  EXPECT_TRUE(eval(person));
  parse("phone.number = '3'");
  EXPECT_TRUE(eval(person));
  parse("phone.number = '4'");
  EXPECT_FALSE(eval(person));
}

TEST_F(PlangTest, Precedence) {
  parse("a = 1 && b = 2 || c = 3 && d < 4 || not e < 5 || f = 6");
  const BinOp& n = dynamic_cast<const BinOp &>(get_parsed());
  EXPECT_EQ(BinOp::OR, n.type());
  const BinOp& n1 = dynamic_cast<const BinOp &>(n.left());
  EXPECT_EQ(BinOp::OR, n1.type()); // a = 1 && b = 2 || c = 3 && d < 4 || not e < 5
  const BinOp& n2 = dynamic_cast<const BinOp &>(n.right());
  EXPECT_EQ(BinOp::EQ, n2.type()); // f = 6
  const BinOp& n11 = dynamic_cast<const BinOp &>(n1.left());
  EXPECT_EQ(BinOp::OR, n11.type()); // a = 1 && b = 2 || c = 3 && d < 4
  const BinOp& n111 = dynamic_cast<const BinOp &>(n11.left());
  EXPECT_EQ(BinOp::AND, n111.type()); // a = 1 && b = 2
  const BinOp& n1111 = dynamic_cast<const BinOp &>(n111.left());
  EXPECT_EQ(BinOp::EQ, n1111.type()); // a = 1
  const BinOp& n1112 = dynamic_cast<const BinOp &>(n111.right());
  EXPECT_EQ(BinOp::EQ, n1112.type()); // b = 2
  const BinOp& n112 = dynamic_cast<const BinOp &>(n11.right());
  EXPECT_EQ(BinOp::AND, n112.type()); // c = 3 && d < 4
  const BinOp& n1121 = dynamic_cast<const BinOp &>(n112.left());
  EXPECT_EQ(BinOp::EQ, n1121.type()); // c = 3
  const BinOp& n1122 = dynamic_cast<const BinOp &>(n112.right());
  EXPECT_EQ(BinOp::LT, n1122.type()); // d < 4
  const BinOp& n12 = dynamic_cast<const BinOp &>(n1.right()); // not e < 5
  EXPECT_EQ(BinOp::NOT, n12.type());
  const BinOp& n121 = dynamic_cast<const BinOp &>(n12.left()); // e < 5
  EXPECT_EQ(BinOp::LT, n121.type());
}

}  // namespace plang
