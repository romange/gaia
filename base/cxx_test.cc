// Copyright 2016, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <functional>
#include <random>

#include "base/gtest.h"
#include "base/logging.h"

namespace base {
using benchmark::DoNotOptimize;
using std::string;

static unsigned LoopSwitch(int val, int cnt) {
  unsigned sum = 0;
  for (int i = 0; i < cnt; ++i) {
      switch (val) {
        case 0: sum += (i >> 3)*i; break;
        case 1: sum += i*5; break;
        default: sum += i*cnt; break;
      }
  }
  return sum;
}

static unsigned SwitchLoop(int val, int cnt) {
  unsigned sum = 0;
  int i = 0;
  switch (val) {
      case 0:
              for (; i < cnt; ++i) sum+= (i >> 3)*i;
              break;
      case 1: for (; i < cnt; ++i) sum += i*5;
              break;
      default: for (; i < cnt; ++i) sum += i*cnt;
              break;
  }
  return sum;
}

class A {
  string val_;
 public:
  A(string val = string()) : val_(val) {
    CONSOLE_INFO << "A::A " << val_;
  }

  A& Then(std::function<void()> f) {
    f();
    return *this;
  }

  A& Next(A b) {
    CONSOLE_INFO << "Next: " << b.val_;
    return *this;
  }

  A& operator,(A b) {
    CONSOLE_INFO << ", " << b.val_;
    return *this;
  }
};


class CxxTest : public testing::Test {
};

TEST_F(CxxTest, LoopSwitch) {
  for (unsigned i = 0; i < 10; ++i) {
    LoopSwitch(1, 10000);
  }
}

template<typename T> void DummyFunc(T&& t) {
  T tmp = std::forward<T>(t);
  (void)tmp;
}

TEST_F(CxxTest, MoveRef) {
  DummyFunc("foo");
  const string bar = "bar";
  DummyFunc(bar);
  EXPECT_EQ("bar", bar);

  string beh = "beh";
  DummyFunc(std::move(beh));
  EXPECT_TRUE(beh.empty());
}

TEST_F(CxxTest, SequenceOrder) {
  A().Next(A("First")).Next(A("Second"));
  (A(), A("First")), A("Second");
}

static void BM_LoopSwitch(benchmark::State& state) {
  int val = state.range(0);
  int cnt = state.range(1);
  unsigned sum = 0;
  while (state.KeepRunning()) {
    sum += LoopSwitch(val, cnt);
  }
  DoNotOptimize(sum);
}
BENCHMARK(BM_LoopSwitch)->ArgPair(1, 10000);

static void BM_SwitchLoop(benchmark::State& state) {
  int val = state.range(0);
  int cnt = state.range(1);
  unsigned sum = 0;
  while (state.KeepRunning()) {
    sum += SwitchLoop(val, cnt);
  }

  DoNotOptimize(sum);
}
BENCHMARK(BM_SwitchLoop)->ArgPair(1, 10000);

}  // namespace base
