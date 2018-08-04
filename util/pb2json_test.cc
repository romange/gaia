// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/pb2json.h"

#include "util/plang/addressbook.pb.h"
#include "base/gtest.h"

namespace util {

using namespace tutorial;
using namespace std;

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

}  // namespace util
