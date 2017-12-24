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

#ifndef STRINGS_UTIL_H_
#define STRINGS_UTIL_H_

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _MSC_VER
// #include <strings.h>  // for strcasecmp, but msvc does not have this header
#endif

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
  return StringPiece(path, p + 1);
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


bool DictionaryParse(const std::string& encoded_str,
                     std::vector<std::pair<std::string, std::string> >* items);
// ----------------------------------------------------------------------
// LowerString()
// UpperString()
//    Convert the characters in "s" to lowercase or uppercase.  ASCII-only:
//    these functions intentionally ignore locale because they are applied to
//    identifiers used in the Protocol Buffer language, not to natural-language
//    strings.
// ----------------------------------------------------------------------

inline void LowerString(std::string* s) {
  std::string::iterator end = s->end();
  for (std::string::iterator i = s->begin(); i != end; ++i) {
    // tolower() changes based on locale.  We don't want this!
    if ('A' <= *i && *i <= 'Z') *i += 'a' - 'A';
  }
}

inline void UpperString(std::string* s) {
  std::string::iterator end = s->end();
  for (std::string::iterator i = s->begin(); i != end; ++i) {
    // toupper() changes based on locale.  We don't want this!
    if ('a' <= *i && *i <= 'z') *i += 'A' - 'a';
  }
}

inline std::string LowerString(std::string s) {
  LowerString(&s);
  return s;
}

inline std::string UpperString(std::string s) {
  UpperString(&s);
  return s;
}

// Replaces all occurrences of substd::string in s with replacement. Returns the
// number of instances replaced. s must be distinct from the other arguments.
//
// Less flexible, but faster, than RE::GlobalReplace().
int GlobalReplaceSubstring(const StringPiece& substring,
                           const StringPiece& replacement,
                           std::string* s);

bool ReplaceSuffix(StringPiece suffix, StringPiece new_suffix, std::string* str);

// Returns whether str begins with prefix.
inline bool HasPrefixString(const StringPiece& str,
                            const StringPiece& prefix) {
  return str.starts_with(prefix);
}

// Returns whether str ends with suffix.
inline bool HasSuffixString(const StringPiece& str,
                            const StringPiece& suffix) {
  return str.ends_with(suffix);
}
void RemoveCharsFromString(StringPiece chars, std::string* str);

}  // namespace strings



// Compares two char* strings for equality. (Works with NULL, which compares
// equal only to another NULL). Useful in hash tables:
//    hash_map<const char*, Value, hash<const char*>, streq> ht;
/*struct streq : public std::binary_function<const char*, const char*, bool> {
  bool operator()(const char* s1, const char* s2) const {
    return ((s1 == 0 && s2 == 0) ||
            (s1 && s2 && *s1 == *s2 && strcmp(s1, s2) == 0));
  }
};*/

// Compares two char* strings. (Works with NULL, which compares greater than any
// non-NULL). Useful in maps:
//    map<const char*, Value, strlt> m;
/*struct strlt : public std::binary_function<const char*, const char*, bool> {
  bool operator()(const char* s1, const char* s2) const {
    return (s1 != s2) && (s2 == 0 || (s1 != 0 && strcmp(s1, s2) < 0));
  }
};
*/

// Returns whether str has only Ascii characters (as defined by ascii_isascii()
// in strings/ascii_ctype.h).
bool IsAscii(const char* str, int len);
inline bool IsAscii(const StringPiece& str) {
  return IsAscii(str.data(), str.size());
}

// Returns the smallest lexicographically larger std::string of equal or smaller
// length. Returns an empty std::string if there is no such successor (if the input
// is empty or consists entirely of 0xff bytes).
// Useful for calculating the smallest lexicographically larger string
// that will not be prefixed by the input string.
//
// Examples:
// "a" -> "b", "aaa" -> "aab", "aa\xff" -> "ab", "\xff" -> "", "" -> ""
// std::string PrefixSuccessor(const StringPiece& prefix);

// Returns the immediate lexicographically-following string. This is useful to
// turn an inclusive range into something that can be used with Bigtable's
// SetLimitRow():
//
//     // Inclusive range [min_element, max_element].
//     std::string min_element = ...;
//     std::string max_element = ...;
//
//     // Equivalent range [range_start, range_end).
//     std::string range_start = min_element;
//     std::string range_end = ImmediateSuccessor(max_element);
//
// WARNING: Returns the input std::string with a '\0' appended; if you call c_str()
// on the result, it will compare equal to s.
//
// WARNING: Transforms "" -> "\0"; this doesn't account for Bigtable's special
// treatment of "" as infinity.
// std::string ImmediateSuccessor(const StringPiece& s);

// Fills in *separator with a short std::string less than limit but greater than or
// equal to start. If limit is greater than start, *separator is the common
// prefix of start and limit, followed by the successor to the next character in
// start. Examples:
// FindShortestSeparator("foobar", "foxhunt", &sep) => sep == "fop"
// FindShortestSeparator("abracadabra", "bacradabra", &sep) => sep == "b"
// If limit is less than or equal to start, fills in *separator with start.
// void FindShortestSeparator(const StringPiece& start, const StringPiece& limit,
  //                         std::string* separator);

// Replaces the first occurrence (if replace_all is false) or all occurrences
// (if replace_all is true) of oldsub in s with newsub. In the second version,
// *res must be distinct from all the other arguments.
std::string StringReplace(const StringPiece& s, const StringPiece& oldsub,
                     const StringPiece& newsub, bool replace_all);
void StringReplace(const StringPiece& s, const StringPiece& oldsub,
                   const StringPiece& newsub, bool replace_all,
                   std::string* res);

// Gets the next token from std::string*stringp, where tokens are strings separated
// by characters from delim.
char* gstrsep(char** stringp, const char* delim);



// Returns whether s contains only whitespace characters (including the case
// where s is empty).
bool OnlyWhitespace(const StringPiece& s);

// Formats a std::string in the same fashion as snprintf(), but returns either the
// number of characters written, or zero if not enough space was available.
// (snprintf() returns the number of characters that would have been written if
// enough space had been available.)
//
// A drop-in replacement for the safe_snprintf() macro.
int SafeSnprintf(char* str, size_t size, const char* format, ...)
    PRINTF_ATTRIBUTE(3, 4);

#endif  // STRINGS_UTIL_H_
