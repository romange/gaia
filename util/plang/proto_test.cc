#include <gtest/gtest.h>
#include "util/plang/addressbook.pb.h"

using namespace google::protobuf;
using tutorial::Person;

class TestServiceImpl : public tutorial::TestService {
public:

 void Test(RpcController* controller,
           const ::tutorial::Person* request,
           ::tutorial::AddressBook* response,
           Closure* done) {
   done->Run();
 }

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

TEST_F(ProtoTest, Rpc) {
  TestServiceImpl t_service;
  tutorial::Person person;
  tutorial::AddressBook address_book;
  t_service.Test(nullptr, &person, &address_book,
                 internal::NewCallback((ProtoTest*)this, &ProtoTest::TestCallback));
  const ServiceDescriptor* s_descr = t_service.GetDescriptor();
  EXPECT_EQ("TestService", s_descr->name());
  EXPECT_EQ("tutorial.TestService", s_descr->full_name());
  const MethodDescriptor* m_descr = s_descr->method(0);
  EXPECT_EQ("Test", m_descr->name());
  EXPECT_EQ("tutorial.TestService.Test", m_descr->full_name());
  EXPECT_TRUE(s_descr->FindMethodByName("Test") == m_descr);
}

TEST_F(ProtoTest, Clear) {
  Arena arena;
  Person* person = Arena::CreateMessage<Person>(&arena);
  person->mutable_account()->set_bank_name("Foo");
  person->Clear();
}
