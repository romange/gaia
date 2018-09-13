// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/varz_stats.h"

#include "base/walltime.h"
#include "strings/strcat.h"
#include "strings/stringprintf.h"

using absl::StrAppend;
using std::string;
using strings::AsString;
using util::VarzValue;

namespace http {

typedef std::lock_guard<std::mutex> mguard;

std::mutex VarzListNode::g_varz_mutex;

static std::string KeyValueWithStyle(StringPiece key, StringPiece val) {
  string res("<span class='key_text'>");
  StrAppend(&res, key, ":</span><span class='value_text'>", val, "</span>\n");
  return res;
}

VarzListNode::VarzListNode(const char* name) : name_(name), prev_(nullptr) {
  mguard guard(g_varz_mutex);
  next_ = global_list();
  if (next_) {
    next_->prev_ = this;
  }
  global_list() = this;
}

VarzListNode::~VarzListNode() {
  mguard guard(g_varz_mutex);
  if (global_list() == this) {
    global_list() = next_;
  } else {
    if (next_) {
      next_->prev_ = prev_;
    }
    if (prev_) {
      prev_->next_ = next_;
    }
  }
}

string VarzListNode::Format(const AnyValue& av, bool is_json) {
  string result;

  if (is_json) {
    switch (av.type) {
      case VarzValue::STRING:
        StrAppend(&result, "\"", av.str, "\"");
        break;
      case VarzValue::NUM:
      case VarzValue::TIME:
        StrAppend(&result, av.num);
        break;
      case VarzValue::DOUBLE:
        StrAppend(&result, av.dbl);
        break;
      case VarzValue::MAP:
        result.append("{ ");
        for (const auto& k_v : av.key_value_array) {
          StrAppend(&result, "\"", k_v.first, "\": ", Format(k_v.second, is_json), ",");
        }
        result.back() = ' ';
        result.append("}");
        break;
    }
  } else {
    switch (av.type) {
      case VarzValue::STRING:
        StrAppend(&result, "<span class='value_text'> ", av.str, " </span>\n");
        break;
      case VarzValue::NUM:
        StrAppend(&result, "<span class='value_text'> ", av.num, " </span>\n");
        break;
      case VarzValue::DOUBLE:
        StrAppend(&result, "<span class='value_text'> ", av.dbl, " </span>\n");
        break;
      case VarzValue::TIME:
        StrAppend(&result, "<span class='value_text'> ", base::PrintLocalTime(av.num), " </span>\n");
        break;
      case VarzValue::MAP:
        for (const auto& k_v : av.key_value_array) {
          StrAppend(&result, KeyValueWithStyle(k_v.first, Format(k_v.second, is_json)));
        }
        break;
    }
  }
  return result;
}

VarzListNode*& VarzListNode::global_list() {
  static VarzListNode* varz_global_list = nullptr;
  return varz_global_list;
}

auto VarzMapCount::ReadLockAndFindOrInsert(StringPiece key) -> Map::iterator {
  rw_spinlock_.lock_shared();
  auto it = map_counts_.find(key);
  if (it != map_counts_.end())
    return it;

  rw_spinlock_.unlock_shared();

  rw_spinlock_.lock();
  auto res = map_counts_.emplace(key, base::atomic_wrapper<long>(0));
  rw_spinlock_.unlock_and_lock_shared();
  return res.first;
}

void VarzMapCount::IncBy(StringPiece key, int32 delta) {
  if (key.empty()) {
    LOG(DFATAL) << "Empty varz key";
    return;
  }

  if (!delta) {
    return;
  }

  auto it = ReadLockAndFindOrInsert(key);
  it->second.fetch_add(delta, std::memory_order_relaxed);
  rw_spinlock_.unlock_shared();
}

void VarzMapCount::Set(StringPiece key, int32 value) {
  if (key.empty()) {
    LOG(DFATAL) << "Empty varz key";
    return;
  }

  auto it = ReadLockAndFindOrInsert(key);
  it->second.store(value, std::memory_order_relaxed);
  rw_spinlock_.unlock_shared();
}

VarzValue VarzMapCount::GetData() const {
  AnyValue::Map result;
  rw_spinlock_.lock_shared();
  for (const auto& k_v : map_counts_) {
    result.emplace_back(AsString(k_v.first), VarzValue::FromInt(k_v.second));
  }
  rw_spinlock_.unlock_shared();
  typedef AnyValue::Map::value_type vt;
  std::sort(result.begin(), result.end(),
            [](const vt& l, const vt& r) { return l.first < r.first; });

  return AnyValue{std::move(result)};
}

VarzValue VarzMapAverage5m::GetData() const {
  std::lock_guard<std::mutex> lock(mutex_);
  AnyValue::Map result;

  for (const auto& k_v : avg_) {
    AnyValue::Map items;
    int64 count = k_v.second.second.Sum();
    int64 sum = k_v.second.first.Sum();
    items.emplace_back("count", VarzValue::FromInt(count));
    items.emplace_back("sum", VarzValue::FromInt(sum));

    double avg = count > 0 ? double(sum) / count : 0;
    items.emplace_back("average", VarzValue::FromDouble(avg));

    result.emplace_back(AsString(k_v.first), AnyValue(items));
  }

  return AnyValue{std::move(result)};
}

void VarzMapAverage5m::IncBy(StringPiece key, int32 delta) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& val = avg_[key];
  val.first.IncBy(delta);
  val.second.Inc();
}

VarzValue VarzCount::GetData() const {
  return VarzValue::FromInt(val_.load());
}

VarzValue VarzQps::GetData() const {
  return VarzValue::FromInt(val_.Get());
}

VarzValue VarzFunction::GetData() const {
  AnyValue::Map result = cb_();
  return AnyValue(result);
}

}  // namespace http
