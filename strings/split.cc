// Copyright 2008 and onwards Google Inc.  All rights reserved.
//
// Maintainer: Greg Miller <jgm@google.com>

#include "strings/split.h"

#include <stdlib.h>
#include <string.h>
#include <iterator>
#include <limits>
#include "base/integral_types.h"

#include "base/logging.h"
#include "base/macros.h"
#include "strings/stringpiece.h"
#include "strings/ascii_ctype.h"

using std::vector;
using std::string;

void SplitCSVLineWithDelimiter(char* line, char delimiter,
                               vector<char*>* cols) {
  char* end_of_line = line + strlen(line);
  char* end;
  char* start;

  for (; line < end_of_line; line++) {
    // Skip leading whitespace, unless said whitespace is the delimiter.
    while (ascii_isspace(*line) && *line != delimiter)
      ++line;

    if (*line == '"' && delimiter == ',') {     // Quoted value...
      start = ++line;
      end = start;
      for (; *line; line++) {
        if (*line == '"') {
          line++;
          if (*line != '"')  // [""] is an escaped ["]
            break;           // but just ["] is end of value
        }
        *end++ = *line;
      }
      // All characters after the closing quote and before the comma
      // are ignored.
      line = strchr(line, delimiter);
      if (!line) line = end_of_line;
    } else {
      start = line;
      line = strchr(line, delimiter);
      if (!line) line = end_of_line;
      // Skip all trailing whitespace, unless said whitespace is the delimiter.
      for (end = line; end > start; --end) {
        if (!ascii_isspace(end[-1]) || end[-1] == delimiter)
          break;
      }
    }
    const bool need_another_column =
      (*line == delimiter) && (line == end_of_line - 1);
    *end = '\0';
    cols->push_back(start);
    // If line was something like [paul,] (comma is the last character
    // and is not proceeded by whitespace or quote) then we are about
    // to eliminate the last column (which is empty). This would be
    // incorrect.
    if (need_another_column)
      cols->push_back(end);

    assert(*line == '\0' || *line == delimiter);
  }
}

void SplitCSVLine(char* line, vector<char*>* cols) {
  SplitCSVLineWithDelimiter(line, ',', cols);
}

void SplitCSVLineWithDelimiterForStrings(const string &line,
                                         char delimiter,
                                         vector<string> *cols) {
  // Unfortunately, the interface requires char* instead of const char*
  // which requires copying the string.
  string cline(line);
  vector<char*> v;
  SplitCSVLineWithDelimiter(&cline.front(), delimiter, &v);
  for (vector<char*>::const_iterator ci = v.begin(); ci != v.end(); ++ci) {
    cols->push_back(*ci);
  }
}

