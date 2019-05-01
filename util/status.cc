// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/status.h"

#include "base/logging.h"

using std::string;

namespace util {

const Status Status::CANCELLED(StatusCode::CANCELLED, "Cancelled");
const Status Status::OK;

void Status::AddErrorMsg(StatusCode::Code code, const std::string& msg) {
  if (!error_detail_) {
    error_detail_.reset(new ErrorDetail{code, msg});
  } else {
    error_detail_->error_msgs.push_back(msg);
  }
  VLOG(2) << msg;
}

void Status::AddErrorMsg(const std::string& msg) {
  AddErrorMsg(StatusCode::INTERNAL_ERROR, msg);
}

void Status::AddError(const Status& status) {
  if (status.ok()) return;
  AddErrorMsg(status.code(), status.ToString());
}

void Status::GetErrorMsgs(std::vector<string>* msgs) const {
  msgs->clear();
  if (error_detail_ != NULL) {
    *msgs = error_detail_->error_msgs;
  }
}

void Status::GetErrorMsg(string* msg) const {
  msg->clear();
  if (error_detail_ != NULL) {
    if (StatusCode::Code_IsValid(error_detail_->error_code)) {
      const string& str = StatusCode::Code_Name(error_detail_->error_code);
      msg->append(str).append(" ");
    }
    for (const string& e : error_detail_->error_msgs) {
      msg->append(e).append("\n");
    }
    if (!error_detail_->error_msgs.empty()) {
      msg->pop_back();
    }
  } else {
    msg->assign("OK");
  }
}

std::ostream& operator<<(std::ostream& o, const Status& status) {
  o << status.ToString();
  return o;
}

}  // namespace util

