// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "absl/strings/str_cat.h"


// char* StrAppend(char* dest, unsigned n, std::initializer_list<absl::AlphaNum> list);
char* StrAppend(char* dest, unsigned n, std::initializer_list<absl::string_view> list);

template<typename...Args> char* StrAppend(char* dest, unsigned n, const absl::AlphaNum& a1, const Args... args) {
    return StrAppend(dest, n, {a1.Piece(), static_cast<const absl::AlphaNum&>(args).Piece()...});
}
