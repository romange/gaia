// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "absl/strings/escaping.h"

namespace strings {

void AppendEncodedUrl(const absl::string_view src, std::string* dest);

}  // namespace strings
