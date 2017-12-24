// Copyright 2008 Google Inc. All Rights Reserved.

#include "strings/charset.h"

#include <string.h>
#include "strings/stringpiece.h"

namespace strings {

CharSet::CharSet() {
  memset(bits_, 0, sizeof(bits_));
}

CharSet::CharSet(StringPiece characters) {
  memset(bits_, 0, sizeof(bits_));
  for (size_t i = 0; i < characters.size(); ++i) {
    Add(characters[i]);
  }
}

}  // namespace strings
