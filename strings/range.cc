/*
 * Copyright 2017 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//
// Modified by Roman Gershman (romange@gmail.com)

#include "strings/range.h"

#include "strings/charset.h"

namespace strings {
namespace detail {

size_t qfind_first_byte_of_charset(const StringPiece haystack,
                                   const StringPiece needles) {
  CharSet s(needles);

  for (size_t index = 0; index < haystack.size(); ++index) {
    if (s.Test(haystack[index])) {
      return index;
    }
  }
  return std::string::npos;
}


size_t qfind_first_not_of_charset(const StringPiece haystack,
                                   const StringPiece needles) {
  CharSet s(needles);

  for (size_t index = 0; index < haystack.size(); ++index) {
    if (!s.Test(haystack[index])) {
      return index;
    }
  }
  return std::string::npos;
}

}  // namespace detail
}  // namespace strings
