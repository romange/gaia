// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/varz_stats.h"

#include "base/walltime.h"
#include "strings/strcat.h"
#include "strings/stringprintf.h"

using std::string;

namespace http {

typedef std::lock_guard<std::mutex> mguard;

static std::mutex g_varz_mutex;

static std::string KeyValueWithStyle(StringPiece key, StringPiece val) {
  string res("<span class='key_text'>");
  StrAppend(&res, key, ":</span><span class='value_text'>", val, "</span>\n");
  return res;
}


VarzListNode::VarzListNode(const char* name)
  : name_(name), prev_(nullptr) {
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

string VarzListNode::AnyValue::Format(bool is_json) const {
  string result;

  if (is_json) {
    switch (type) {
      case STRING:
        StrAppend(&result, "\"", str, "\"");
      break;
      case NUM:
      case TIME:
        StrAppend(&result, num);
      break;
      case DOUBLE_T:
        StrAppend(&result, dbl);
      break;
      case MAP:
        result.append("{ ");
        for (const auto& k_v : key_value_array) {
          StrAppend(&result, "\"", k_v.first, "\": ", k_v.second.Format(is_json), ",");
        }
        result.back() = ' ';
        result.append("}");
      break;
    }
  } else {
     switch (type) {
      case STRING:
        StrAppend(&result, "<span class='value_text'> ", str, " </span>\n");
      break;
      case NUM:
        StrAppend(&result, "<span class='value_text'> ", num, " </span>\n");
      break;
      case DOUBLE_T:
        StrAppend(&result, "<span class='value_text'> ", dbl, " </span>\n");
      break;
      case TIME:
        StrAppend(&result, "<span class='value_text'> ", base::PrintLocalTime(num), " </span>\n");
      break;
      case MAP:
        for (const auto& k_v : key_value_array) {
          StrAppend(&result, KeyValueWithStyle(k_v.first, k_v.second.Format(is_json)));
        }
      break;
    }
  }
  return result;
}

VarzListNode* & VarzListNode::global_list() {
  static VarzListNode* varz_global_list = nullptr;
  return varz_global_list;
}

void VarzListNode::IterateValues(
  std::function<void(const std::string&, const std::string&)> cb, bool is_json) {
  {
    mguard guard(g_varz_mutex);
    for (VarzListNode* node = global_list(); node != nullptr; node = node->next_) {
      if (node->name_ != nullptr) {
        cb(node->name_, node->GetData().Format(is_json));
      }
    }
  }
}

void VarzMapCount::IncBy(StringPiece key, int32 delta) {
  if (key.empty()) {
    LOG(DFATAL) << "Empty varz key";
    return;
  }

  if (!delta) {
    return;
  }

  rw_spinlock_.lock_shared();
  auto it = map_counts_.find(key);
  if (it == map_counts_.end()) {
    rw_spinlock_.unlock_shared();

    rw_spinlock_.lock();
    auto res = map_counts_.emplace(key, base::atomic_wrapper<long>(delta));
    if (res.second) {
      rw_spinlock_.unlock();
      return;
    }
    rw_spinlock_.unlock_and_lock_shared();
    it = res.first;
  }
  it->second.fetch_add(delta, std::memory_order_relaxed);
  rw_spinlock_.unlock_shared();
}


VarzListNode::AnyValue VarzMapCount::GetData() const {
  AnyValue::Map result;
  rw_spinlock_.lock_shared();
  for (const auto& k_v : map_counts_) {
    result.emplace_back(k_v.first.as_string(), AnyValue(k_v.second));
  }
  rw_spinlock_.unlock_shared();
  typedef AnyValue::Map::value_type vt;
  std::sort(result.begin(), result.end(), [](const vt& l, const vt& r) {
    return l.first < r.first;
  });

  return AnyValue(std::move(result));
}

VarzListNode::AnyValue VarzMapAverage5m::GetData() const {
  std::lock_guard<std::mutex> lock(mutex_);
  AnyValue::Map result;

  for (const auto& k_v : avg_) {
    AnyValue::Map items;
    int64 count = k_v.second.second.Sum();
    int64 sum = k_v.second.first.Sum();
    items.emplace_back("count", AnyValue(count));
    items.emplace_back("sum", AnyValue(sum));

    double avg = count > 0 ? double(sum) / count : 0;
    items.emplace_back("average", AnyValue(avg));

    result.emplace_back(k_v.first.as_string(), AnyValue(items));
  }

  return AnyValue(std::move(result));
}

void VarzMapAverage5m::IncBy(StringPiece key, int32 delta) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& val = avg_[key];
  val.first.IncBy(delta);
  val.second.Inc();
}

VarzListNode::AnyValue VarzCount::GetData() const {
  return AnyValue(val_.load());
}

VarzListNode::AnyValue VarzQps::GetData() const {
  return AnyValue(int64(val_.Get()));
}

VarzListNode::AnyValue VarzFunction::GetData() const {
  AnyValue::Map result = cb_();
  return AnyValue(result);
}

}  // namespace http
