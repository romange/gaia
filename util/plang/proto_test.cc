#include <gtest/gtest.h>
#include "util/plang/addressbook.pb.h"

using namespace google::protobuf;
using tutorial::Person;

class TestServiceImpl : public tutorial::TestService {
public:

};

class ProtoTest : public testing::Test {
public:
  void TestCallback() {
  }
};

TEST_F(ProtoTest, Basic) {
  tutorial::Person person;
  person.set_name("Roman");
}

TEST_F(ProtoTest, Clear) {
  Arena arena;
  Person* person = Arena::CreateMessage<Person>(&arena);
  person->mutable_account()->set_bank_name("Foo");
  EXPECT_EQ(&arena, person->GetArena());

  Person::PhoneNumber* pn = new Person::PhoneNumber;
  person->mutable_phone()->AddAllocated(pn);
  EXPECT_EQ("Foo", person->account().bank_name());
  person->Clear();
}
