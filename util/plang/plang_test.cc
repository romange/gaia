// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/plang/plang.h"

#include "util/plang/addressbook.pb.h"
#include "util/plang/plang_parser.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace plang {

using namespace tutorial;

class PlangTest : public testing::Test {
protected:
  void scan(const std::string& val) {
    istr_.str(val);
    scanner_.reset(new Scanner(istr_, ostr_));
  }

  int parse(const std::string& val) {
    istr_.str(val);
    parser_.reset(new Parser(istr_, ostr_));
    return parser_->parse();
  }

  std::istringstream istr_;
  std::ostringstream ostr_;
  std::unique_ptr<Scanner> scanner_;
  std::unique_ptr<Parser> parser_;
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
    BinOp op(BinOp::EQ, new IntLiteral(IntLiteral::Signed(6)),
             new StringTerm("id", StringTerm::VARIABLE));
    op.eval(person, res_setter);
    ASSERT_EQ(ExprValue::CPPTYPE_BOOL, res.type);
    EXPECT_TRUE(res.val.int_val);
  }
}

TEST_F(PlangTest, Scanner) {
  scan("ABCD 1234 a123+6");
  int token = scanner_->lex();
  EXPECT_EQ(ParserBase::IDENTIFIER, token);
  EXPECT_EQ("ABCD", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(ParserBase::NUMBER, token);
  EXPECT_EQ("1234", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(ParserBase::IDENTIFIER, token);
  EXPECT_EQ("a123", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(int('+'), token);
  EXPECT_EQ("+", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(ParserBase::NUMBER, token);
  EXPECT_EQ("6", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(0, token);

  scan("a=b");
  token = scanner_->lex();
  EXPECT_EQ(ParserBase::IDENTIFIER, token);
  token = scanner_->lex();
  EXPECT_EQ(int('='), token);
  token = scanner_->lex();
  EXPECT_EQ(ParserBase::IDENTIFIER, token);
}

TEST_F(PlangTest, ScanString) {
  scan("\"Foo\"Bar\"End");
  int token = scanner_->lex();
  EXPECT_EQ(ParserBase::STRING, token);
  EXPECT_EQ("Foo", scanner_->matched());

  token = scanner_->lex();
  EXPECT_EQ(ParserBase::IDENTIFIER, token);
  EXPECT_EQ("Bar", scanner_->matched());
  token = scanner_->lex();
  EXPECT_EQ(0, token);
  EXPECT_EQ("End", ostr_.str());  // Unmatched rule.

  const char kVar[] = "field_one.field_two";
  scan(kVar);
  token = scanner_->lex();
  EXPECT_EQ(ParserBase::IDENTIFIER, token);
  EXPECT_EQ(kVar, scanner_->matched());
}

TEST_F(PlangTest, Parse) {
  Person person;
  person.set_name("Roman");
  person.set_id(6);
  parse("\"Foo\" = \"Bar\"");
  EXPECT_TRUE(parser_->res_val != nullptr);
  EXPECT_FALSE(parser_->eval(person));

  parse("name = \"Bar\"");
  EXPECT_FALSE(parser_->eval(person));

  parse("name = \"Roman\"");
  EXPECT_TRUE(parser_->eval(person));

  parse("id=6");
  EXPECT_TRUE(parser_->eval(person));

  parse("(id=6) AND name=\"Roman\"");
  EXPECT_TRUE(parser_->eval(person));

  parse("(id=6) or name=\"Foo\"");
  EXPECT_TRUE(parser_->eval(person));

  person.set_id(5);
  EXPECT_FALSE(parser_->eval(person));

  person.set_name("Foo");
  EXPECT_TRUE(parser_->eval(person));
}

TEST_F(PlangTest, Comparison) {
  Person person;
  person.set_id(6);
  parse("id < 7");
  ASSERT_TRUE(parser_->res_val != nullptr);
  EXPECT_TRUE(parser_->eval(person));
  parse("id > 5");
  EXPECT_TRUE(parser_->eval(person));
  parse("id > 6");
  EXPECT_FALSE(parser_->eval(person));
  parse("not id > 6");
  EXPECT_TRUE(parser_->eval(person));
  parse("id != 5");
  EXPECT_TRUE(parser_->eval(person));
}

TEST_F(PlangTest, Function) {
  Person person;
  person.set_id(6);
  /*parse("id(7,8) = 5");
  ASSERT_TRUE(parser_->res_val != nullptr);

  parse("def(id)");
  ASSERT_TRUE(parser_->res_val != nullptr);
  EXPECT_TRUE(parser_->eval(person));

  parse("def(phone)");
  EXPECT_FALSE(parser_->eval(person));

  parse("def(account.bank_name)");
  EXPECT_FALSE(parser_->eval(person));*/

  parse("def(account.address.street)");
  EXPECT_FALSE(parser_->eval(person));
}

TEST_F(PlangTest, Enum) {
  Person::PhoneNumber phone;
  phone.set_type(Person::HOME);
  parse("type = 'HOME'");
  EXPECT_TRUE(parser_->eval(phone));

  parse("type = 1");
  EXPECT_TRUE(parser_->eval(phone));

  parse("type = 'MOBILE'");
  EXPECT_FALSE(parser_->eval(phone));
}

TEST_F(PlangTest, RepeatedMsgs) {
  Person person;
  person.add_phone()->set_number("1");
  person.add_phone()->set_number("2");
  person.add_phone()->set_number("3");
  parse("phone.number = '1'");
  EXPECT_TRUE(parser_->eval(person));
  parse("phone.number = '3'");
  EXPECT_TRUE(parser_->eval(person));
  parse("phone.number = '4'");
  EXPECT_FALSE(parser_->eval(person));
}
}  // namespace plang
