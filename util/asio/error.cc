// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/asio/error.h"
#include "absl/strings/str_cat.h"

namespace util {

using namespace boost;
using namespace std;

namespace {

class gaia_error_category : public system::error_category {
 public:
  const char* name() const noexcept final { return "gaia.util"; }

  std::string message(int ev) const override {
    switch (static_cast<gaia_error>(ev)) {
      case gaia_error::bad_header:
        return "bad header";
      case gaia_error::invalid_version:
        return "invalid header version";
      default:
        return absl::StrCat("gaia.util error(", ev, ")");
    }
  }

  system::error_condition default_error_condition(int ev) const noexcept override {
    return system::error_condition{ev, *this};
  }

  bool equivalent(int ev, system::error_condition const& condition) const noexcept override {
    return condition.value() == ev && &condition.category() == this;
  }

  bool equivalent(system::error_code const& error, int ev) const noexcept override {
    return error.value() == ev && &error.category() == this;
  }
};

}  // namespace

system::error_code make_error_code(gaia_error ev) {
  static const gaia_error_category cat;
  return system::error_code{static_cast<int>(ev), cat};
}

}  // namespace util
