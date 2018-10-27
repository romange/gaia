// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

namespace util {


// Runs sh with the command. Returns 0 if succeeded. Child status is ignored.
int sh_exec(const char* cmd);

}  // namespace util
