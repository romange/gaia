// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "absl/strings/str_cat.h"


char* StrAppend(char* dest, unsigned n, std::initializer_list<absl::AlphaNum> list);
