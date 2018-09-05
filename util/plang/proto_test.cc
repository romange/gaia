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
  person->Clear();
}
