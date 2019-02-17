// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <iosfwd>
#include <functional>

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;

static bool log_new = false;
void* operator new(std::size_t n) {
	if (log_new)
		cerr << "Allocating " << n << " bytes" << endl;
	return malloc(n);
}
void operator delete(void* p, size_t sz) {
	free(p);
}

void operator delete(void* p) noexcept {
	free(p);
}

namespace base {

class LambdaTest : public testing::Test {
};

#pragma clang diagnostic ignored "-Wunused-lambda-capture"

TEST_F(LambdaTest, Cb) {
	std::array<int, 1024> arr;
	arr.fill(5);
	log_new = true;

  auto cb = [arr] {};
	static_assert(sizeof(cb) > 4000, "");
	cb();
	std::function<void()> f1 = cb;
	static_assert(sizeof(f1) == 32, "");

	std::array<char, 16> arr2;
	arr2.fill(5);

	std::function<void()> f2 = [arr2] {};

	log_new = false;
}



}  // namespace base
