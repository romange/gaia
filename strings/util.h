//
// Copyright 1999-2006 and onwards Google, Inc.
//
// Useful std::string functions and so forth.  This is a grab-bag file.
//
// You might also want to look at memutil.h, which holds mem*()
// equivalents of a lot of the str*() functions in string.h,
// eg memstr, mempbrk, etc.
//
// These functions work fine for UTF-8 strings as long as you can
// consider them to be just byte strings.  For example, due to the
// design of UTF-8 you do not need to worry about accidental matches,
// as long as all your inputs are valid UTF-8 (use \uHHHH, not \xHH or \oOOO).
//
// Caveats:
// * all the lengths in these routines refer to byte counts,
//   not character counts.
// * case-insensitivity in these routines assumes that all the letters
//   in question are in the range A-Z or a-z.
//
// If you need Unicode specific processing (for example being aware of
// Unicode character boundaries, or knowledge of Unicode casing rules,
// or various forms of equivalence and normalization), take a look at
// files in i18n/utf8.

#pragma once


#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <functional>
#include <string>
#include <vector>

#include "base/integral_types.h"
#include "base/port.h"
#include "strings/stringpiece.h"

// Newer functions.

namespace strings {

inline StringPiece GetFileExtension(StringPiece path) {
  auto p = path.rfind('.');
  if (p == StringPiece::npos) return StringPiece();
  path.remove_prefix(p + 1);
  return path;
}

// Finds the next end-of-line sequence.
// An end-of-line sequence is one of:
//   \n    common on unix, including mac os x
//   \r    common on macos 9 and before
//   \r\n  common on windows
//
// Returns a StringPiece that contains the end-of-line sequence (a pointer into
// the input, 1 or 2 characters long).
//
// If the input does not contain an end-of-line sequence, returns an empty
// StringPiece located at the end of the input:
//    StringPiece(sp.data() + sp.length(), 0).

StringPiece FindEol(StringPiece sp);

}  // namespace strings

