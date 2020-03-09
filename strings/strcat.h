// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "absl/strings/str_cat.h"

/**
 * @brief Appends a list of string arguments to the destination buffer. Does not cross the
 *        specified max capacity. After the append operation is completed sets the null character
 *        at the end of the string and returns the pointer to it.
 *
 * @param dest - the destination buffer
 * @param n    - its maximum capacity.
 * @param list - list of string arguments.
 * @return char* - a pointer to the end of the string (\0).
 */
char* StrAppend(char* dest, unsigned n, std::initializer_list<absl::string_view> list);

template<typename...Args> char* StrAppend(char* dest, unsigned n, const absl::AlphaNum& a1,
                                          const Args... args) {
    return StrAppend(dest, n, {a1.Piece(), static_cast<const absl::AlphaNum&>(args).Piece()...});
}
