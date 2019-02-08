// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <string>
#include <vector>

#include "base/macros.h"
#include "util/status.pb.h"

namespace util {

class Status {
 public:
  constexpr Status() noexcept : error_detail_(NULL) {}

  // copy c'tor makes copy of error detail so Status can be returned by value
  Status(const Status& status) : error_detail_(
    status.error_detail_ != NULL ? new ErrorDetail(*status.error_detail_) : NULL) {
  }

  // Move c'tor.
  Status(Status&& st) noexcept : error_detail_(st.error_detail_) {
    st.error_detail_ = nullptr;
  }

  // c'tor for error case - is this useful for anything other than CANCELLED?
  Status(StatusCode::Code code)
    : error_detail_(new ErrorDetail(code)) {
  }

  // c'tor for error case
  Status(StatusCode::Code code, std::string error_msg)
    : error_detail_(new ErrorDetail(code, std::move(error_msg))) {
  }

  // c'tor for internal error
  Status(std::string error_msg) : error_detail_(
    new ErrorDetail(StatusCode::INTERNAL_ERROR, std::move(error_msg))) {}

  ~Status() {
    if (error_detail_ != NULL) delete error_detail_;
  }

  // same as copy c'tor
  Status& operator=(const Status& status) {
    delete error_detail_;
    if (LIKELY(status.error_detail_ == NULL)) {
      error_detail_ = NULL;
    } else {
      error_detail_ = new ErrorDetail(*status.error_detail_);
    }
    return *this;
  }

  Status& operator=(Status&& status) noexcept {
    std::swap(error_detail_, status.error_detail_);
    return *this;
  }

  bool ok() const { return error_detail_ == NULL; }

  bool IsCancelled() const {
    return error_detail_ != NULL
        && error_detail_->error_code == StatusCode::CANCELLED;
  }

  // Does nothing if status.ok().
  // Otherwise: if 'this' is an error status, adds the error msg from 'status;
  // otherwise assigns 'status'.
  void AddError(const Status& status);
  void AddErrorMsg(StatusCode::Code code, const std::string& msg);
  void AddErrorMsg(const std::string& msg);
  void GetErrorMsgs(std::vector<std::string>* msgs) const;
  void GetErrorMsg(std::string* msg) const;

  std::string ToString() const {
    std::string msg; GetErrorMsg(&msg);
    return msg;
  }

  StatusCode::Code code() const {
    return error_detail_ == NULL ? StatusCode::OK : error_detail_->error_code;
  }

  static const Status OK;
  static const Status CANCELLED;

  friend std::ostream& operator<<(std::ostream& o, const Status& status);
 private:

  static Status ByCode(StatusCode::Code code) { return Status(code); }
  struct ErrorDetail {
    StatusCode::Code error_code;  // anything other than OK
    std::vector<std::string> error_msgs;

    ErrorDetail(StatusCode::Code code)
      : error_code(code) {}
    ErrorDetail(StatusCode::Code code, std::string msg)
      : error_code(code) {
        error_msgs.push_back(std::move(msg));
    }
  };

  ErrorDetail* error_detail_;
};


// Sometimes functions need to return both data object and status.
// It's inconvenient to set this data object by reference via argument parameter.
// StatusObject should help with this problem.
template<typename T> struct StatusObject {
  Status status;
  T obj;

  bool ok() const { return status.ok(); }

  StatusObject() noexcept = default;
  StatusObject(StatusObject&&) noexcept = default;
  StatusObject(const StatusObject& st) = default;

  StatusObject(const Status& s) : status(s), obj() {}
  StatusObject(Status&& s) noexcept : status(std::move(s)), obj() {}

  StatusObject(const T& t) : obj(t) {}
  StatusObject(T&& t) : obj(std::move(t)) {}

  StatusObject& operator=(StatusObject&&) noexcept = default;
  StatusObject& operator=(const StatusObject&) = default;

  std::string ToString() const { return status.ToString(); }
};


// some generally useful macros
#define RETURN_IF_ERROR(stmt) \
  do { \
    auto __status__ = (stmt); \
    if (UNLIKELY(!__status__.ok())) return __status__; \
  } while (false)

#define CHECK_STATUS(stmt) \
  do { \
    auto __status__ = (stmt); \
    CHECK(__status__.ok()) << __status__; \
  } while (false)


#define GET_UNLESS_ERROR(var, stmt) \
  decltype((stmt).obj) var; \
  do { \
    auto __res__ = (stmt); \
    if (UNLIKELY(!__res__.ok())) return __res__.status; \
    var = std::move(__res__.obj); \
  } while (false)


}  // namespace util
