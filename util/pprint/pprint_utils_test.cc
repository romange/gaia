#include "util/pprint/pprint_utils.h"

#include "util/pprint/pprint_utils_test.pb.h"
#include <gtest/gtest.h>

namespace util {
namespace pprint {

class PprintUtilsTest : public testing::Test {
};

TEST_F(PprintUtilsTest, SizeSummarizerSimpleString) {
  SimpleString m1;
  m1.set_simple("ori");
  SimpleString m2;
  m2.set_simple("alex");
  SimpleString m3;
  m3.set_simple("constantinopol");

  SizeSummarizer ss(m1.GetDescriptor());
  ss.AddSizes(m1);
  ss.AddSizes(m2);
  ss.AddSizes(m3);

  ASSERT_EQ(m1.simple().size() + m2.simple().size() + m3.simple().size(),
            ss.GetSizes().begin()->second);
}

TEST_F(PprintUtilsTest, SizeSummarizerRepeatingString) {
  RepeatingString m1;
  RepeatingString m2;
  RepeatingString m3;
  m2.add_repeating("ori");
  m3.add_repeating("alex");
  m3.add_repeating("sasha");

  SizeSummarizer ss(m1.GetDescriptor());
  ss.AddSizes(m1);
  ss.AddSizes(m2);
  ss.AddSizes(m3);

  ASSERT_EQ(m2.repeating(0).size() + m3.repeating(0).size() + m3.repeating(1).size(),
            ss.GetSizes().begin()->second);
}

TEST_F(PprintUtilsTest, SizeSummarizerNestedString) {
  NestedString m1;
  NestedString m2;

  m1.mutable_a1()->set_s1("hello");
  m1.mutable_a1()->add_s2("ori");
  m1.mutable_a1()->add_s2("alex");

  m1.add_a2();
  m1.add_a2();
  m1.mutable_a2()->Mutable(0)->set_s1("a");
  m1.mutable_a2()->Mutable(0)->add_s2("bb");
  m1.mutable_a2()->Mutable(1)->set_s1("ccc");
  m1.mutable_c1()->set_s1(",xjnzcv");
  m1.mutable_c1()->set_s2("zxc");
  m1.mutable_c1()->mutable_d1()->set_s1("z");
  m1.mutable_c1()->mutable_d1()->set_s2("zcvx");


  m2.add_a2();
  m2.add_a2();
  m2.add_a2();
  m2.mutable_a2()->Mutable(0)->set_s1("Z");
  m2.mutable_a2()->Mutable(0)->add_s2("RR");
  m2.mutable_a2()->Mutable(1)->set_s1("YYYYY");
  m2.mutable_a2()->Mutable(1)->add_s2("RRRRRR");
  m2.mutable_a2()->Mutable(1)->add_s2("R");
  m2.mutable_a2()->Mutable(1)->add_s2("RR");
  m2.mutable_a2()->Mutable(2)->set_s1("XXX");
  m2.add_b1();
  m2.add_b1();
  m2.add_b1();
  m2.mutable_b1()->Mutable(0)->set_s1("AAA");
  m2.mutable_b1()->Mutable(0)->add_s2("XXXXXXXRK");
  m2.mutable_b1()->Mutable(0)->set_s3("AAAAA");
  m2.mutable_b1()->Mutable(1)->set_s1("AAAA");
  m2.mutable_b1()->Mutable(1)->add_s2("XXXX");
  m2.mutable_b1()->Mutable(1)->add_s2("XX");
  m2.mutable_b1()->Mutable(1)->set_s3("AAAAAA");
  m2.mutable_b1()->Mutable(2)->set_s1("AAAAA");
  m2.mutable_b1()->Mutable(2)->add_s2("RRRRR");
  m2.mutable_b1()->Mutable(2)->add_s2("RRR");
  m2.mutable_b1()->Mutable(2)->add_s2("R");
  m2.mutable_b1()->Mutable(2)->add_s2("");
  m2.mutable_b1()->Mutable(2)->set_s3("AAAAAAA");
  m2.mutable_c1()->set_s1("ASDASD,xjnzcv");
  m2.mutable_c1()->set_s2("qwerqwerzxc");
  m2.mutable_c1()->mutable_d1()->set_s1("-_z");
  m2.mutable_c1()->mutable_d1()->set_s2("-_----zcvx");

  SizeSummarizer ss(m1.GetDescriptor());
  ss.AddSizes(m1);
  ss.AddSizes(m2);
  std::map<std::string, size_t> sizes = ss.GetSizes();
  auto expected_a1_s1 = m1.a1().s1().size() + m2.a1().s1().size();
  auto expected_a1_s2 = m1.a1().s2(0).size() + m1.a1().s2(1).size();
  auto expected_a2_s1 = (m1.a2(0).s1().size() + m1.a2(1).s1().size() +
                         m2.a2(0).s1().size() + m2.a2(1).s1().size() + m2.a2(2).s1().size());
  auto expected_a2_s2 = (m1.a2(0).s2(0).size() +
                         m2.a2(0).s2(0).size() +
                         m2.a2(1).s2(0).size() + m2.a2(1).s2(1).size() + m2.a2(1).s2(2).size());
  auto expected_b1_s1 = m2.b1(0).s1().size() + m2.b1(1).s1().size() + m2.b1(2).s1().size();
  auto expected_b1_s2 = (m2.b1(0).s2(0).size() +
                         m2.b1(1).s2(0).size() + m2.b1(1).s2(1).size() +
                         (m2.b1(2).s2(0).size() + m2.b1(2).s2(1).size() +
                          m2.b1(2).s2(2).size() + m2.b1(2).s2(3).size()));
  auto expected_b1_s3 = m2.b1(0).s3().size() + m2.b1(1).s3().size() + m2.b1(2).s3().size();
  auto expected_c1_s1 = m1.c1().s1().size() + m2.c1().s1().size();
  auto expected_c1_s2 = m1.c1().s2().size() + m2.c1().s2().size();
  auto expected_c1_d1_s1 = m1.c1().d1().s1().size() + m2.c1().d1().s1().size();
  auto expected_c1_d1_s2 = m1.c1().d1().s2().size() + m2.c1().d1().s2().size();
  auto expected_c1_d1 = expected_c1_d1_s1 + expected_c1_d1_s2;
  ASSERT_EQ(sizes["a1.s1"], expected_a1_s1);
  ASSERT_EQ(sizes["a1.s2"], expected_a1_s2);
  ASSERT_EQ(sizes["a2.s1"], expected_a2_s1);
  ASSERT_EQ(sizes["a2.s2"], expected_a2_s2);
  ASSERT_EQ(sizes["b1.s1"], expected_b1_s1);
  ASSERT_EQ(sizes["b1.s2"], expected_b1_s2);
  ASSERT_EQ(sizes["b1.s3"], expected_b1_s3);
  ASSERT_EQ(sizes["c1.s1"], expected_c1_s1);
  ASSERT_EQ(sizes["c1.s2"], expected_c1_s2);
  ASSERT_EQ(sizes["c1.d1.s1"], expected_c1_d1_s1);
  ASSERT_EQ(sizes["c1.d1.s2"], expected_c1_d1_s2);
  ASSERT_EQ(sizes["c1.d1"], expected_c1_d1);
  ASSERT_EQ(sizes["a1"],expected_a1_s1 + expected_a1_s2);
  ASSERT_EQ(sizes["a2"],expected_a2_s1 + expected_a2_s2);
  ASSERT_EQ(sizes["b1"],expected_b1_s1 + expected_b1_s2 + expected_b1_s3);
  ASSERT_EQ(sizes["c1"],expected_c1_s1 + expected_c1_s2 + expected_c1_d1);
}

}
}
