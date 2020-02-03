// Copyright 2010 Google Inc. All Rights Reserved.
// Maintainer: mec@google.com (Michael Chastain)
//
// Convert strings to numbers or numbers to strings.
//
// Modified by roman
//
#pragma once

#include "absl/strings/numbers.h"

#include <cinttypes>
#include <cstring>
#include <ctime>
#include <functional>
#include <limits>
#include <string>
#include <vector>

#include "base/integral_types.h"

#include "base/port.h"  // for MUST_USE_RESULT

#include "strings/stringpiece.h"

// Convert strings to numeric values, with strict error checking.
// Leading and trailing spaces are not allowed.
// Negative inputs are not allowed for unsigned ints (unlike strtoul).
// Numbers must be in base 10; see the _base variants below for other bases.
// Returns false on errors (including overflow/underflow).
inline bool safe_strto32(StringPiece str, int32* value) {
  return absl::SimpleAtoi<int32>(str, value);
}

inline bool safe_strto64(StringPiece str, int64* value) {
  return absl::SimpleAtoi<int64>(str, value);
}
inline bool safe_strtou32(StringPiece str, uint32* value) {
  return absl::SimpleAtoi<uint32>(str, value);
}
inline bool safe_strtou64(StringPiece str, uint64* value) {
  return absl::SimpleAtoi<uint64>(str, value);
}

bool safe_strto32_base(StringPiece str, int32* v, int base);
bool safe_strtou32_base(StringPiece str, uint32* v, int base);
bool safe_strto64_base(StringPiece str, int64* v, int base);
bool safe_strtou64_base(StringPiece str, uint64* v, int base);

void u64tostr(uint64 val, size_t out_len, char *buf, unsigned base);

// Convert strings to floating point values.
// Leading and trailing spaces are allowed.
// Values may be rounded on over- and underflow.
bool safe_strtof(StringPiece str, float* value);
bool safe_strtod(StringPiece str, double* value);


char* FastHex64ToBuffer(uint64 i, char* buffer);

#if 0
// ----------------------------------------------------------------------
// FastIntToBuffer()
// FastHexToBuffer()
// FastHex64ToBuffer()
// FastHex32ToBuffer()
// FastTimeToBuffer()
//    These are intended for speed.  FastIntToBuffer() assumes the
//    integer is non-negative.  FastHexToBuffer() puts output in
//    hex rather than decimal.  FastTimeToBuffer() puts the output
//    into RFC822 format.
//
//    FastHex64ToBuffer() puts a 64-bit unsigned value in hex-format,
//    padded to exactly 16 bytes (plus one byte for '\0')
//
//    FastHex32ToBuffer() puts a 32-bit unsigned value in hex-format,
//    padded to exactly 8 bytes (plus one byte for '\0')
//
//    All functions take the output buffer as an arg.  FastInt() uses
//    at most 22 bytes, FastTime() uses exactly 30 bytes.  They all
//    return a pointer to the beginning of the output, which for
//    FastHex() may not be the beginning of the input buffer.  (For
//    all others, we guarantee that it is.)
//
//    NOTE: In 64-bit land, sizeof(time_t) is 8, so it is possible
//    to pass to FastTimeToBuffer() a time whose year cannot be
//    represented in 4 digits. In this case, the output buffer
//    will contain the string "Invalid:<value>"
// ----------------------------------------------------------------------

// Previously documented minimums -- the buffers provided must be at least this
// long, though these numbers are subject to change:
//     Int32, UInt32:        12 bytes
//     Int64, UInt64, Hex:   22 bytes
//     Time:                 30 bytes
//     Hex32:                 9 bytes
//     Hex64:                17 bytes
// Use kFastToBufferSize rather than hardcoding constants.
static const int kFastToBufferSize = 32;

char* FastInt32ToBuffer(int32 i, char* buffer);
char* FastInt64ToBuffer(int64 i, char* buffer);
char* FastUInt32ToBuffer(uint32 i, char* buffer);
char* FastUInt64ToBuffer(uint64 i, char* buffer);
char* FastHexToBuffer(int i, char* buffer) MUST_USE_RESULT;
char* FastTimeToBuffer(time_t t, char* buffer);
char* FastHex64ToBuffer(uint64 i, char* buffer);
char* FastHex32ToBuffer(uint32 i, char* buffer);

// at least 22 bytes long
inline char* FastIntToBuffer(int i, char* buffer) {
  return (sizeof(i) == 4 ?
          FastInt32ToBuffer(i, buffer) : FastInt64ToBuffer(i, buffer));
}
inline char* FastUIntToBuffer(unsigned int i, char* buffer) {
  return (sizeof(i) == 4 ?
          FastUInt32ToBuffer(i, buffer) : FastUInt64ToBuffer(i, buffer));
}

// ----------------------------------------------------------------------
// FastInt32ToBufferLeft()
// FastUInt32ToBufferLeft()
// FastInt64ToBufferLeft()
// FastUInt64ToBufferLeft()
//
// Like the Fast*ToBuffer() functions above, these are intended for speed.
// Unlike the Fast*ToBuffer() functions, however, these functions write
// their output to the beginning of the buffer (hence the name, as the
// output is left-aligned).  The caller is responsible for ensuring that
// the buffer has enough space to hold the output.
//
// Returns a pointer to the end of the string (i.e. the null character
// terminating the string).
// ----------------------------------------------------------------------

char* FastInt32ToBufferLeft(int32 i, char* buffer);    // at least 12 bytes
char* FastUInt32ToBufferLeft(uint32 i, char* buffer);    // at least 12 bytes
char* FastInt64ToBufferLeft(int64 i, char* buffer);    // at least 22 bytes
char* FastUInt64ToBufferLeft(uint64 i, char* buffer);    // at least 22 bytes

// Just define these in terms of the above.
inline char* FastUInt32ToBuffer(uint32 i, char* buffer) {
  FastUInt32ToBufferLeft(i, buffer);
  return buffer;
}
inline char* FastUInt64ToBuffer(uint64 i, char* buffer) {
  FastUInt64ToBufferLeft(i, buffer);
  return buffer;
}

// ----------------------------------------------------------------------
// HexDigitsPrefix()
//  returns 1 if buf is prefixed by "num_digits" of hex digits
//  returns 0 otherwise.
//  The function checks for '\0' for string termination.
// ----------------------------------------------------------------------
int HexDigitsPrefix(const char* buf, int num_digits);

// ----------------------------------------------------------------------
// ConsumeStrayLeadingZeroes
//    Eliminates all leading zeroes (unless the string itself is composed
//    of nothing but zeroes, in which case one is kept: 0...0 becomes 0).
void ConsumeStrayLeadingZeroes(std::string* str);

#endif

// ----------------------------------------------------------------------
// ParseLeadingInt32Value
//    A simple parser for int32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    This cannot handle decimal numbers with leading 0s, since they will be
//    treated as octal.  If you know it's decimal, use ParseLeadingDec32Value.
// --------------------------------------------------------------------
int32 ParseLeadingInt32Value(StringPiece str, int32 deflt);
/*inline int32 ParseLeadingInt32Value(const std::string& str, int32 deflt) {
  return ParseLeadingInt32Value(str.c_str(), deflt);
}*/

// ParseLeadingUInt32Value
//    A simple parser for uint32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    This cannot handle decimal numbers with leading 0s, since they will be
//    treated as octal.  If you know it's decimal, use ParseLeadingUDec32Value.
// --------------------------------------------------------------------
uint32 ParseLeadingUInt32Value(StringPiece str, uint32 deflt);

// ----------------------------------------------------------------------
// ParseLeadingDec32Value
//    A simple parser for decimal int32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    The string passed in is treated as *10 based*.
//    This can handle strings with leading 0s.
//    See also: ParseLeadingDec64Value
// --------------------------------------------------------------------
int32 ParseLeadingDec32Value(StringPiece str, int32 deflt);

// ParseLeadingUDec32Value
//    A simple parser for decimal uint32 values. Returns the parsed value
//    if a valid integer is found; else returns deflt. It does not
//    check if str is entirely consumed.
//    The string passed in is treated as *10 based*.
//    This can handle strings with leading 0s.
//    See also: ParseLeadingUDec64Value
// --------------------------------------------------------------------
uint32 ParseLeadingUDec32Value(StringPiece str, uint32 deflt);

// ----------------------------------------------------------------------
// ParseLeadingUInt64Value
// ParseLeadingInt64Value
// ParseLeadingHex64Value
// ParseLeadingDec64Value
// ParseLeadingUDec64Value
//    A simple parser for long long values.
//    Returns the parsed value if a
//    valid integer is found; else returns deflt
// --------------------------------------------------------------------
uint64 ParseLeadingUInt64Value(StringPiece str, uint64 deflt);

int64 ParseLeadingInt64Value(StringPiece str, int64 deflt);

uint64 ParseLeadingHex64Value(StringPiece str, uint64 deflt);

int64 ParseLeadingDec64Value(StringPiece str, int64 deflt);

uint64 ParseLeadingUDec64Value(StringPiece str, uint64 deflt);

// ----------------------------------------------------------------------
// ParseLeadingDoubleValue
//    A simple parser for double values. Returns the parsed value
//    if a valid double is found; else returns deflt. It does not
//    check if str is entirely consumed.
// --------------------------------------------------------------------
double ParseLeadingDoubleValue(const char* str, double deflt);
inline double ParseLeadingDoubleValue(const std::string& str, double deflt) {
  return ParseLeadingDoubleValue(str.c_str(), deflt);
}

// ----------------------------------------------------------------------
// ParseLeadingBoolValue()
//    A recognizer of boolean string values. Returns the parsed value
//    if a valid value is found; else returns deflt.  This skips leading
//    whitespace, is case insensitive, and recognizes these forms:
//    0/1, false/true, no/yes, n/y
// --------------------------------------------------------------------
bool ParseLeadingBoolValue(const char* str, bool deflt);
inline bool ParseLeadingBoolValue(const std::string& str, bool deflt) {
  return ParseLeadingBoolValue(str.c_str(), deflt);
}

#if 0
// ----------------------------------------------------------------------
// AutoDigitStrCmp
// AutoDigitLessThan
// StrictAutoDigitLessThan
// autodigit_less
// autodigit_greater
// strict_autodigit_less
// strict_autodigit_greater
//    These are like less<string> and greater<string>, except when a
//    run of digits is encountered at corresponding points in the two
//    arguments.  Such digit strings are compared numerically instead
//    of lexicographically.  Therefore if you sort by
//    "autodigit_less", some machine names might get sorted as:
//        exaf1
//        exaf2
//        exaf10
//    When using "strict" comparison (AutoDigitStrCmp with the strict flag
//    set to true, or the strict version of the other functions),
//    strings that represent equal numbers will not be considered equal if
//    the string representations are not identical.  That is, "01" < "1" in
//    strict mode, but "01" == "1" otherwise.
// ----------------------------------------------------------------------

int AutoDigitStrCmp(const char* a, int alen,
                    const char* b, int blen,
                    bool strict);

bool AutoDigitLessThan(const char* a, int alen,
                       const char* b, int blen);

bool StrictAutoDigitLessThan(const char* a, int alen,
                             const char* b, int blen);

struct autodigit_less
  : public std::binary_function<const std::string&, const std::string&, bool> {
  bool operator()(const std::string& a, const std::string& b) const {
    return AutoDigitLessThan(a.data(), a.size(), b.data(), b.size());
  }
};

struct autodigit_greater
  : public std::binary_function<const std::string&, const std::string&, bool> {
  bool operator()(const std::string& a, const std::string& b) const {
    return AutoDigitLessThan(b.data(), b.size(), a.data(), a.size());
  }
};

struct strict_autodigit_less
  : public std::binary_function<const std::string&, const std::string&, bool> {
  bool operator()(const std::string& a, const std::string& b) const {
    return StrictAutoDigitLessThan(a.data(), a.size(), b.data(), b.size());
  }
};

struct strict_autodigit_greater
  : public std::binary_function<const std::string&, const std::string&, bool> {
  bool operator()(const std::string& a, const std::string& b) const {
    return StrictAutoDigitLessThan(b.data(), b.size(), a.data(), a.size());
  }
};


#endif

// ----------------------------------------------------------------------
// SimpleDtoa()
// SimpleFtoa()
// DoubleToBuffer()
// FloatToBuffer()
//    Description: converts a double or float to a string which, if
//    passed to strtod(), will produce the exact same original double
//    (except in case of NaN; all NaNs are considered the same value).
//    We try to keep the string short but it's not guaranteed to be as
//    short as possible.
//
//    DoubleToBuffer() and FloatToBuffer() write the text to the given
//    buffer and return it.  The buffer must be at least
//    kDoubleToBufferSize bytes for doubles and kFloatToBufferSize
//    bytes for floats.  kFastToBufferSize is also guaranteed to be large
//    enough to hold either.
//
//    Return value: string
// ----------------------------------------------------------------------
std::string SimpleDtoa(double value);
std::string SimpleFtoa(float value);

char* DoubleToBuffer(double i, char* buffer);
char* FloatToBuffer(float i, char* buffer);

// In practice, doubles should never need more than 24 bytes and floats
// should never need more than 14 (including null terminators), but we
// overestimate to be safe.
static const int kDoubleToBufferSize = 32;
static const int kFloatToBufferSize = 24;
