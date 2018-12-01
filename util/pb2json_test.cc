// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/pb2json.h"

#include "util/plang/addressbook.pb.h"
#include "base/gtest.h"
#include "absl/strings/str_cat.h"

namespace util {

using namespace tutorial;
using namespace std;
using namespace google::protobuf;

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

}  // namespace util
