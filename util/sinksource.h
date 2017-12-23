// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#ifndef UTIL_SINKSOURCE_H
#define UTIL_SINKSOURCE_H

#include <memory>
#include <string>
#include "base/integral_types.h"
#include "base/pod_array.h"

#include "strings/stringpiece.h"
#include "util/status.h"

// We prefer Sink and Source (like in snappy and icu) over ZeroCopy streams like in protobuf.
// The reason for this is not convenient corner cases where you have few bytes left in the buffers
// returned by ZeroCopy streams and you need to write a special code in order to serialize your
// primitives whcih require more space.
// Sinks solve this problem by allowing scratch buffers.

namespace util {

class Sink {
 public:
  typedef strings::MutableByteRange WritableBuffer;

  Sink() {}
  virtual ~Sink() {}

  // Appends slice to sink.
  virtual Status Append(const strings::ByteRange& slice) = 0;

  // Returns a writable buffer for appending .
  // Guarantees that result.capacity >=min_capacity.
  // May return a pointer to the caller-owned scratch buffer which must have capacity >=min_capacity.
  // The returned buffer is only valid until the next operation on this Sink.
  // After writing at most result.capacity bytes, call Append() with the pointer returned from this
  // function (result.first) and the number of bytes written. Many Append() implementations will
  // avoid copying bytes if this function returned an internal buffer (by just returning
  // WritableBuffer object or calling its Prefix function.)
  // Partial usage example:
  // WritableBuffer buf = sink->GetAppendBuffer(min_capacity, scracth_buffer);
  // ... Write n bytes into buf, with n <= buf.capacity.
  // sink->Append(buf.Prefix(n));
  // In many implementations, that call to Append will avoid copying bytes.
  // If the Sink allocates or reallocates an internal buffer, it should use the desired_capacity_hint
  // if appropriate.
  // If a non-scratch buffer is returned, the caller may only pass its prefix to Append().
  // That is, it is not correct to pass an interior pointer to Append().
  // The default implementation always returns the scratch buffer.
  virtual WritableBuffer GetAppendBuffer(
    size_t min_capacity,
    WritableBuffer scratch,
    size_t desired_capacity_hint = 0);

  // Flushes internal buffers. The default implemenation does nothing. Sink
  // subclasses may use internal buffers that require calling Flush() at the end
  // of writing to the stream.
  virtual Status Flush();

 private:
  DISALLOW_COPY_AND_ASSIGN(Sink);
};


class ZeroCopySink : public Sink {
 public:
  explicit ZeroCopySink(void* dest) : dest_(reinterpret_cast<uint8*>(dest)) {}

  Status Append(const strings::ByteRange& slice) {
    memcpy(dest_ + offset_, slice.data(), slice.size());
    offset_ += slice.size();
    return Status::OK;
  }

  size_t written() const { return offset_; }
 private:
  size_t offset_ = 0;
  uint8* dest_;
};

class StringSink : public Sink {
  std::string contents_;
public:
  Status Append(const strings::ByteRange& slice) {
    contents_.append(strings::charptr(slice.data()), slice.size());
    return Status::OK;
  }

  std::string& contents() { return contents_; }
  const std::string& contents() const { return contents_; }
};



// Source classes. Allow synchronous reads from an abstract source.
class Source {
 public:
  Source() {}
  virtual ~Source() {}


  StatusObject<size_t> Read(const strings::MutableByteRange& range);

  void Prepend(const strings::ByteRange& range) {
    prepend_buf_.insert(prepend_buf_.begin(), range.begin(), range.end());
  }

 protected:
  virtual StatusObject<size_t> ReadInternal(const strings::MutableByteRange& range) = 0;

 private:

  DISALLOW_COPY_AND_ASSIGN(Source);

  base::PODArray<uint8> prepend_buf_;
};

class StringSource : public Source {
public:
  // block_size is used to simulate paging reads, usually in tests.
  // input must exists all the time StringSource is used. It should not be
  // changed either since StringSource wraps its internal buffer during the construction.
  explicit StringSource(const std::string& input, uint32 block_size = kuint32max)
    : input_(input), block_size_(block_size) {}

  size_t Available() const {return input_.size();};

private:
  StatusObject<size_t> ReadInternal(const strings::MutableByteRange& range) override;

  StringPiece input_;
  uint32 block_size_;
};


}  // namespace util

#endif  // UTIL_SINKSOURCE_H
