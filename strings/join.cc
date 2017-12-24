// Copyright 2008 and onwards Google Inc.  All rights reserved.

#include "strings/join.h"

#include <memory>
#include "base/logging.h"
#include "strings/ascii_ctype.h"
#include "strings/escaping.h"

using std::vector;

// ----------------------------------------------------------------------
// JoinUsing()
//    This merges a vector of string components with delim inserted
//    as separaters between components.
//    This is essentially the same as JoinUsingToBuffer except
//    the return result is dynamically allocated using "new char[]".
//    It is the caller's responsibility to "delete []" the
//
//    If result_length_p is not NULL, it will contain the length of the
//    result string (not including the trailing '\0').
// ----------------------------------------------------------------------
char* JoinUsing(const vector<const char*>& components,
                const char* delim,
                int*  result_length_p) {
  const int num_components = components.size();
  const int delim_length = strlen(delim);
  int num_chars = (num_components > 1)
                ? delim_length * (num_components - 1)
                : 0;
  for (int i = 0; i < num_components; ++i)
    num_chars += strlen(components[i]);

  char* res_buffer = new char[num_chars+1];
  return JoinUsingToBuffer(components, delim, num_chars+1,
                           res_buffer, result_length_p);
}

// ----------------------------------------------------------------------
// JoinUsingToBuffer()
//    This merges a vector of string components with delim inserted
//    as separaters between components.
//    User supplies the result buffer with specified buffer size.
//    The result is also returned for convenience.
//
//    If result_length_p is not NULL, it will contain the length of the
//    result string (not including the trailing '\0').
// ----------------------------------------------------------------------
char* JoinUsingToBuffer(const vector<const char*>& components,
                         const char* delim,
                         int result_buffer_size,
                         char* result_buffer,
                         int*  result_length_p) {
  CHECK(result_buffer != NULL);
  const int num_components = components.size();
  const int max_str_len = result_buffer_size - 1;
  char* curr_dest = result_buffer;
  int num_chars = 0;
  for (int i = 0; (i < num_components) && (num_chars < max_str_len); ++i) {
    const char* curr_src = components[i];
    while ((*curr_src != '\0') && (num_chars < max_str_len)) {
      *curr_dest = *curr_src;
      ++num_chars;
      ++curr_dest;
      ++curr_src;
    }
    if (i != (num_components-1)) {  // not the last component ==> add separator
      curr_src = delim;
      while ((*curr_src != '\0') && (num_chars < max_str_len)) {
        *curr_dest = *curr_src;
        ++num_chars;
        ++curr_dest;
        ++curr_src;
      }
    }
  }

  if (result_buffer_size > 0)
    *curr_dest = '\0';  // add null termination
  if (result_length_p != NULL)  // set string length value
    *result_length_p = num_chars;

  return result_buffer;
}

// ----------------------------------------------------------------------
// JoinStrings()
//    This merges a vector of string components with delim inserted
//    as separaters between components.
//    This is essentially the same as JoinUsingToBuffer except
//    it uses strings instead of char *s.
//
// ----------------------------------------------------------------------

void JoinStringsInArray(string const *components,
                        int num_components,
                        const char *delim,
                        string *result) {
  JoinStringsIterator(components,
                      components + num_components,
                      delim,
                      result);
}
