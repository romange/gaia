// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <boost/system/system_error.hpp>

namespace util {

enum class gaia_error {
  bad_header = 1,
  invalid_version = 2,
};

::boost::system::error_code
make_error_code(gaia_error ev);

}  // namespace util

namespace boost {
namespace system {
template<>
struct is_error_code_enum<::util::gaia_error>
{
    static bool const value = true;
};

} // system
} // boost
