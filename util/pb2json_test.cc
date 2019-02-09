// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/pb2json.h"

#include <gmock/gmock.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/util/message_differencer.h>

#include "absl/strings/str_cat.h"
#include "base/gtest.h"
#include "util/plang/addressbook.pb.h"

namespace tutorial {

// To print nice Protobufs.
void PrintTo(const Person& src, std::ostream* os) { *os << src.DebugString(); }

}  // namespace tutorial

namespace util {

using namespace tutorial;
using namespace std;
using namespace google::protobuf;
namespace gpb = ::google::protobuf;

MATCHER(StatusOk, string(negation ? "is not" : "is") + " ok\n") {
  return arg.ok();
}

// Matches fields specified in str to msg. Ignores other fields in msg.
// Outputs the differences into `diff` in human-readable-format.
// Returns true if no differences were found, false otherwise.
bool ProtoMatchToHelper(const ::google::protobuf::Message& msg, const std::string& str,
                        std::string* diff);

MATCHER_P(ProtoMatchTo, expected,
          std::string(negation ? "is not" : "is") + " equal to:\n" + expected) {
  std::string diff;
  bool res = ProtoMatchToHelper(arg, expected, &diff);
  if (!res)
    *result_listener << "\nDifference found: " << diff;
  return res;
}

bool ProtoMatchToHelper(const gpb::Message& src, const std::string& str, string* diff) {
  using namespace gpb::util;

  std::unique_ptr<gpb::Message> tmp(src.New());
  gpb::TextFormat::Parser parser;
  parser.AllowPartialMessage(true);
  bool parsed = parser.ParseFromString(str, tmp.get());
  if (!parsed) {
    *diff = absl::StrCat("Invalid string for message ", src.GetTypeName(), ":\n", str);

    return false;
  }

  MessageDifferencer differ;
  differ.ReportDifferencesToString(diff);
  differ.set_scope(MessageDifferencer::PARTIAL);
  differ.set_message_field_comparison(MessageDifferencer::EQUIVALENT);

  return differ.Compare(*tmp, src);
}

class Pb2JsonTest : public testing::Test {
 protected:
};

TEST_F(Pb2JsonTest, Basic) {
  Person person;
  person.set_name("Roman");
  person.mutable_account()->set_bank_name("Leumi");
  person.set_id(5);

  string res = Pb2Json(person);
  EXPECT_EQ(R"({"name":"Roman","id":5,"account":{"bank_name":"Leumi"},"dval":0.0})", res);
}

TEST_F(Pb2JsonTest, Unicode) {
  Person person;
  person.set_name("Роман");
  person.mutable_account()->set_bank_name("לאומי");
  person.set_id(5);

  string res = Pb2Json(person);
  EXPECT_EQ(R"({"name":"Роман","id":5,"account":{"bank_name":"לאומי"},"dval":0.0})", res);
}

TEST_F(Pb2JsonTest, Escape) {
  Person person;
  person.set_name("\x01\"");
  person.mutable_account()->set_bank_name("\\");
  person.set_id(5);

  string res = Pb2Json(person);
  EXPECT_EQ(R"({"name":"\u0001\"","id":5,"account":{"bank_name":"\\"},"dval":0.0})", res);
}

const char* kExpected = R"({"name":"","id":0,"phone":[{"number":"1","type":"HOME"},)"
                        R"({"number":"2","type":"WORK"}],"tag":["good","young"],"dval":0.0})";

TEST_F(Pb2JsonTest, EnumAndRepeated) {
  Person p;
  Person::PhoneNumber* n = p.add_phone();
  n->set_type(Person::HOME);
  n->set_number("1");
  n = p.add_phone();
  n->set_type(Person::WORK);
  n->set_number("2");
  p.add_tag("good");
  p.add_tag("young");
  string res = Pb2Json(p);

  EXPECT_EQ(kExpected, res);
}

TEST_F(Pb2JsonTest, Double) {
  static_assert(26.100000381f == 26.1f, "");
  absl::AlphaNum al(26.1f);
  EXPECT_EQ(4, al.size());

  Person p;
  p.set_fval(26.1f);
  string res = Pb2Json(p);

  // We use RawValue with AlphaNum internally to allow correct float outputs.
  EXPECT_EQ(R"({"name":"","id":0,"dval":0.0,"fval":26.1})", res);
}

TEST_F(Pb2JsonTest, Options) {
  AddressBook book;
  book.set_fd1(1);
  Pb2JsonOptions options;

  options.field_name_cb = [](const FieldDescriptor& fd) {
    const FieldOptions& fo = fd.options();
    return fo.HasExtension(fd_name) ? fo.GetExtension(fd_name) : fd.name();
  };
  string res = Pb2Json(book, options);
  EXPECT_EQ(R"({"another_name":1})", res);

  options.enum_as_ints = true;
  Person::PhoneNumber pnum;
  pnum.set_type(Person::WORK);
  res = Pb2Json(pnum, options);
  EXPECT_EQ(R"({"number":"","type":2})", res);

  options = Pb2JsonOptions();
  options.bool_as_int = [](const FieldDescriptor& fd) {
    return fd.name() == "bval" ? true : false;
  };
  JsonParse jp;
  jp.set_bval(true);
  res = Pb2Json(jp, options);
  EXPECT_EQ(R"({"bval":1})", res);
}

TEST_F(Pb2JsonTest, ParseBasic) {
  Person person;
  auto status =
      Json2Pb(R"({"name": "Roman", "id": 5, "account": {"bank_name": "Leumi"}})", &person);
  ASSERT_THAT(status, StatusOk());
  ASSERT_THAT(person, ProtoMatchTo(R"(
    name: "Roman" id: 5 account { bank_name: "Leumi" }
  )"));
}

#if TBD

TEST_F(Pb2JsonTest, ParseExt) {
  Person person;
  auto status = Json2Pb(kExpected, &person);
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(R"(name: "" id: 0 phone { number: "1" type: HOME } phone { number: "2" type: WORK } )"
            R"(tag: "good" tag: "young")",
            person.ShortDebugString());
}

TEST_F(Pb2JsonTest, ParseUnicode) {
  Person person;
  auto status = Json2Pb(R"({"name": "Rom\u0061n",})", &person, false);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ("Roman", person.name());
}

TEST_F(Pb2JsonTest, ParseBool) {
  JsonParse jparse;
  auto status = Json2Pb(R"({"bval": true,})", &jparse, false);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(true, jparse.bval());

  status = Json2Pb(R"({"bval": false,})", &jparse, false);
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(jparse.bval());

  status = Json2Pb(R"({"bval": 1,})", &jparse, false);
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(jparse.bval());

  status = Json2Pb(R"({"bval": 0,})", &jparse, false);
  ASSERT_TRUE(status.ok());
  EXPECT_FALSE(jparse.bval());

  status = Json2Pb(R"({"bval": 2,})", &jparse, false);
  ASSERT_FALSE(status.ok());
}
#endif

}  // namespace util
