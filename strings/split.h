// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "absl/strings/str_split.h"


void SplitCSVLineWithDelimiter(char* line, char delimiter,
                               std::vector<char*>* cols);
