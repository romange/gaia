// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <cstddef>
#include <cstdint>

namespace base {

// [ptr, ptr+len) should be inside memory range with alignment at least 16, i.e. in some cases
// it will access memory before ptr and after ptr + len.
// Returns how many times val appeared in the range.
size_t CountVal8(const uint8_t* ptr, size_t len, char val);

// Writes to buffer the successive differences of buffer
// (buffer[0]-starting_point, buffer[1]-buffer[2], ...)
void ComputeDeltasInplace(uint32_t * buffer, size_t length, uint32_t starting_point);
void ComputeDeltasInplace(uint16_t * buffer, size_t length, uint16_t starting_point);

}  // namespace base
