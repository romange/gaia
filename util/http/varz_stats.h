// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef VARZ_STATS_H
#define VARZ_STATS_H

#include <atomic>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <string>
#include "base/integral_types.h"
#include "base/RWSpinLock.h"
#include "base/atomic_wrapper.h"
#include "strings/stringpiece.h"
#include "strings/unique_strings.h"
#include "strings/strcat.h"
#include "util/stats/sliding_counter.h"

#define DEFINE_VARZ(type, name) http::type name(#name)

namespace http {

class VarzListNode {
public:

  explicit VarzListNode(const char* name);
  virtual ~VarzListNode();

  // Appends string representations of each active node in the list to res.
  // Used for outputting the current state.
  static void IterateValues(std::function<void(const std::string&, const std::string&)> cb,
                            bool is_json);

  struct AnyValue {
    typedef std::vector<std::pair<std::string, AnyValue>> Map;

    int64 num;
    double dbl;
    Map key_value_array;
    std::string str;
    enum {NUM, STRING, MAP, DOUBLE_T, TIME} type;

    AnyValue(int64 n, bool is_time = false) : num(n), type(is_time ? TIME : NUM) {}
    AnyValue(std::string s) : str(std::move(s)), type(STRING) {}
    AnyValue(double d) : dbl(d), type(DOUBLE_T) {}

    AnyValue(Map s) : key_value_array(std::move(s)), type(MAP) {}

    std::string Format(bool is_json) const;
  };

protected:
  virtual AnyValue GetData() const = 0;

  const char* name_;

private:
  // Returns the head to varz linked list. Note that the list becomes invalid after at least one
  // linked list node was destroyed.
  static VarzListNode* & global_list();

  VarzListNode* next_;
  VarzListNode* prev_;
};


/**
  Represents a family (map) of counters. Each counter has its own key name.
**/
class VarzMapCount : public VarzListNode {
public:
  explicit VarzMapCount(const char* varname) : VarzListNode(varname) {
    map_counts_.set_empty_key(StringPiece());
  }

  // Increments key by delta.
  void IncBy(StringPiece key, int32 delta);

  void Inc(StringPiece key) { IncBy(key, 1); }

private:
  virtual AnyValue GetData() const override;

  mutable folly::RWSpinLock rw_spinlock_;
  StringPieceDenseMap<base::atomic_wrapper<long>> map_counts_;
};


// represents a family of averages over 5min period.
class VarzMapAverage5m : public VarzListNode {
public:
  explicit VarzMapAverage5m(const char* varname) : VarzListNode(varname) {
    avg_.set_empty_key(StringPiece());
  }

  void IncBy(StringPiece key, int32 delta);

private:
  virtual AnyValue GetData() const override;

  mutable std::mutex mutex_;

  typedef util::SlidingSecondCounterT<int64, 5, 60> Counter;
  StringPieceDenseMap<std::pair<Counter, Counter>> avg_;
};


class VarzCount : public VarzListNode {
public:
  explicit VarzCount(const char* varname) : VarzListNode(varname) {}

  void IncBy(int32 delta) { val_ += delta; }
  void Inc() { IncBy(1); }

private:
  virtual AnyValue GetData() const override;

  std::atomic_long val_;
};


class VarzQps : public VarzListNode {
public:
  explicit VarzQps(const char* varname) : VarzListNode(varname) {}

  void Inc() { val_.Inc(); }

private:
  virtual AnyValue GetData() const override;

  mutable util::QPSCount val_;
};

class VarzFunction : public VarzListNode {
public:
  typedef AnyValue::Map KeyValMap;
  typedef std::function<KeyValMap()> MapCb;

  // cb - function that formats the output either as json or html according to the boolean is_json.
  explicit VarzFunction(const char* varname, MapCb cb)
    : VarzListNode(varname), cb_(cb) {}


private:
  AnyValue GetData() const override;

  MapCb cb_;
};

// Increments non-trivial key in VarzMapCount described by base and suffix.
// Does it efficiently and avoids allocations.
// The caller must make sure that N is large enough to contain the key.
template<int N> class FastVarMapCounter {
  VarzMapCount& map_count_;

  char buf_[N];
  char* suffix_;
public:
  FastVarMapCounter(VarzMapCount* map_count, std::initializer_list<AlphaNum> base)
      : map_count_(*map_count) {
    suffix_ = StrAppend(buf_, N, base);
  }

  void Inc(const char* suffix) { IncBy(suffix, 1); }

  void IncBy(const char* suffix, int32 val) {
    strcpy(suffix_, suffix);
    map_count_.IncBy(buf_, val);
  }
};

}  // namespace http

#endif  // VARZ_STATS_H