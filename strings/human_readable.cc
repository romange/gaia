// Copyright 2007 Google Inc. All Rights Reserved.

#define __STDC_FORMAT_MACROS 1

#include "strings/human_readable.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "base/logging.h"
#include "strings/stringprintf.h"
#include "strings/strip.h"

using absl::StripLeadingAsciiWhitespace;

namespace {

template <typename T>
const char* GetNegStr(T* value) {
  if (*value < 0) {
    *value = -(*value);
    return "-";
  } else {
    return "";
  }
}

}  // namespace

bool HumanReadableNumBytes::LessThan(const string &a, const string &b) {
  int64 a_bytes, b_bytes;
  if (!HumanReadableNumBytes::ToInt64(a, &a_bytes))
    a_bytes = 0;
  if (!HumanReadableNumBytes::ToInt64(b, &b_bytes))
    b_bytes = 0;
  return (a_bytes < b_bytes);
}

bool HumanReadableNumBytes::ToInt64(const string &str, int64 *num_bytes) {
  const char *cstr = str.c_str();
  bool neg = (*cstr == '-');
  if (neg) {
    cstr++;
  }
  char *end;
  double d = strtod(cstr, &end);
  // If this didn't consume the entire string, fail.
  if (end + 1 < str.c_str() + str.size())
    return false;
  int64 scale = 1;
  switch (*end) {
    // NB: an int64 can only go up to <8 EB.
    case 'E':  scale <<= 10;   // Fall through...
    case 'P':  scale <<= 10;
    case 'T':  scale <<= 10;
    case 'G':  scale <<= 10;
    case 'M':  scale <<= 10;
    case 'K':
    case 'k':  scale <<= 10;
    case 'B':
    case '\0': break;          // To here.
    default:
      return false;
  }
  d *= scale;
  if (d > kint64max || d < 0)
    return false;
  *num_bytes = static_cast<int64>(d + 0.5);
  if (neg) {
    *num_bytes = -*num_bytes;
  }
  return true;
}

bool HumanReadableNumBytes::ToDouble(const string &str, double *num_bytes) {
  char *end;
  double d = strtod(str.c_str(), &end);
  // If this didn't consume the entire string, fail.
  if (end + 1 < str.c_str() + str.size())
    return false;
  const char scale = *end;
  switch (scale) {
    case 'Y':  d *= 1024.0;   // That's a yotta bytes!
    case 'Z':  d *= 1024.0;
    case 'E':  d *= 1024.0;
    case 'P':  d *= 1024.0;
    case 'T':  d *= 1024.0;
    case 'G':  d *= 1024.0;
    case 'M':  d *= 1024.0;
    case 'K':
    case 'k':  d *= 1024.0;
    case 'B':
    case '\0': break;         // to here.
    default:
      return false;
  }
  *num_bytes = d;
  return true;
}

#define arrlen(x) std::extent<decltype(x)>::value

string HumanReadableNumBytes::DoubleToString(double num_bytes) {
  const char *neg_str = GetNegStr(&num_bytes);
  static const char units[] = "BKMGTPEZY";
  double scaled = num_bytes;
  size_t i = 0;
  static_assert(arrlen(units) == 10, "");
  for (; i < arrlen(units) && scaled >= 1024.0; ++i) {
    scaled /= 1024.0;
  }
  if (i == arraysize(units)) {
    return StringPrintf("%s%g", neg_str, num_bytes);
  } else {
    return StringPrintf("%s%.2f%c", neg_str, scaled, units[i]);
  }
}

string HumanReadableNumBytes::ToString(int64 num_bytes) {
  if (num_bytes == kint64min) {
    // Special case for number with not representable nagation.
    return "-8E";
  }

  const char *neg_str = GetNegStr(&num_bytes);

  // Special case for bytes.
  if (num_bytes < GG_LONGLONG(1024)) {
    // No fractions for bytes.
    return StringPrintf("%s%" PRId64 "dB", neg_str, num_bytes);
  }

  static const char units[] = "KMGTPE";  // int64 only goes up to E.
  const char* unit = units;
  while (num_bytes >= GG_LONGLONG(1024) * GG_LONGLONG(1024)) {
    num_bytes /= GG_LONGLONG(1024);
    ++unit;
    CHECK(unit < units + arraysize(units));
  }

  return StringPrintf(((*unit == 'K')
                       ? "%s%.1f%c"
                       : "%s%.2f%c"), neg_str, num_bytes / 1024.0, *unit);
}

string HumanReadableNumBytes::ToStringWithoutRounding(int64 num_bytes) {
  if (num_bytes == kint64min) {
    // Special case for number with not representable nagation.
    return "-8E";
  }

  const char *neg_str = GetNegStr(&num_bytes);
  static const char units[] = "BKMGTPE";  // int64 only goes up to E.

  int64 num_units = num_bytes;
  size_t unit_type = 0;
  for (; unit_type < arraysize(units); unit_type++) {
    if (num_units % 1024 != 0) {
      // Not divisible by the next unit.
      break;
    }

    int64 next_units = num_units >> 10;
    if (next_units == 0) {
      // Less than the next unit.
      break;
    }

    num_units = next_units;
  }
  return StringPrintf("%s%" PRId64 "%c", neg_str, num_units, units[unit_type]);
}

string HumanReadableInt::ToString(int64 value) {
  string s;
  if (value < 0) {
    s += "-";
    value = -value;
  }
  if (value < GG_LONGLONG(1000)) {
    StringAppendF(&s, "%" PRId64, value);
  } else if (value >= GG_LONGLONG(1000000000000000)) {
    // Number bigger than 1E15; use that notation.
    StringAppendF(&s, "%0.3G", static_cast<double>(value));
  } else {
    static const char units[] = "kMBT";
    const char *unit = units;
    while (value >= GG_LONGLONG(1000000)) {
      value /= GG_LONGLONG(1000);
      ++unit;
      CHECK(unit < units + arraysize(units));
    }
    StringAppendF(&s, "%.2f%c", value / 1000.0, *unit);
  }
  return s;
}

string HumanReadableNum::ToString(int64 value) {
  return HumanReadableInt::ToString(value);
}

string HumanReadableNum::DoubleToString(double value) {
  string s;
  if (value < 0) {
    s += "-";
    value = -value;
  }
  if (value < 1.0) {
    StringAppendF(&s, "%.3f", value);
  } else if (value < 10) {
    StringAppendF(&s, "%.2f", value);
  } else if (value < 1e2) {
    StringAppendF(&s, "%.1f", value);
  } else if (value < 1e3) {
    StringAppendF(&s, "%.0f", value);
  } else if (value >= 1e15) {
    // Number bigger than 1E15; use that notation.
    StringAppendF(&s, "%0.3G", value);
  } else {
    static const char units[] = "kMBT";
    const char *unit = units;
    while (value >= 1e6) {
      value /= 1e3;
      ++unit;
      CHECK(unit < units + arraysize(units));
    }
    StringAppendF(&s, "%.2f%c", value / 1000.0, *unit);
  }
  return s;
}

bool HumanReadableNum::ToDouble(const string &str, double *value) {
  char *end;
  double d = strtod(str.c_str(), &end);
  // Allow the string to contain at most one extra character:
  if (end + 1 < str.c_str() + str.size())
    return false;
  const char scale = *end;
  if ((scale == 'k') || (scale == 'K')) {
    d *= 1e3;
  } else if (scale == 'M') {
    d *= 1e6;
  } else if (scale == 'B') {
    d *= 1e9;
  } else if (scale == 'T') {
    d *= 1e12;
  } else if (scale != '\0') {
    return false;
  }
  *value = d;
  return true;
}

bool HumanReadableInt::ToInt64(const string &str, int64 *value) {
  char *end;
  double d = strtod(str.c_str(), &end);
  if (d > kint64max || d < kint64min)
    return false;
  if (*end == 'k') {
    d *= 1000;
  } else if (*end == 'M') {
    d *= 1e6;
  } else if (*end == 'B') {
    d *= 1e9;
  } else if (*end == 'T') {
    d *= 1e12;
  } else if (*end != '\0') {
    return false;
  }
  *value = static_cast<int64>(d < 0 ? d - 0.5 : d + 0.5);
  return true;
}

// Abbreviations used here are acceptable English abbreviations
// without the ending period (".") for brevity, except for uncommon
// abbreviations, in which case the entire word is spelled out. ("mo"
// and "mos" are not good abbreviations for "months" -- with or
// without the period). If needed, one can add a
// HumanReadableTime::ToStringShort() for shorter abbreviations or one
// for always spelling out the unit, HumanReadableTime::ToStringLong().
string HumanReadableElapsedTime::ToShortString(double seconds) {
  string human_readable;

  if (seconds < 0) {
    human_readable = "-";
    seconds = -seconds;
  }

  // Start with us and keep going up to years.
  if (seconds < 0.001) {
    StringAppendF(&human_readable, "%0.3g us", seconds * 1000000.0);
    return human_readable;
  }
  if (seconds < 1.0) {
    StringAppendF(&human_readable, "%0.3g ms", seconds * 1000.0);
    return human_readable;
  }
  if (seconds < 60.0) {
    StringAppendF(&human_readable, "%0.3g s", seconds);
    return human_readable;
  }
  seconds /= 60.0;
  if (seconds < 60.0) {
    StringAppendF(&human_readable, "%0.3g min", seconds);
    return human_readable;
  }
  seconds /= 60.0;
  if (seconds < 24.0) {
    StringAppendF(&human_readable, "%0.3g h", seconds);
    return human_readable;
  }
  seconds /= 24.0;
  if (seconds < 30.0) {
    StringAppendF(&human_readable, "%0.3g days", seconds);
    return human_readable;
  }
  if (seconds < 365.2425) {
    StringAppendF(&human_readable, "%0.3g months", seconds / 30.436875);
    return human_readable;
  }
  seconds /= 365.2425;
  StringAppendF(&human_readable, "%0.3g years", seconds);
  return human_readable;
}
