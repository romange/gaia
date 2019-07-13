// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/logging.h"
#include "mr/mr_pb.h"
#include "util/pb2json.h"

namespace mr3 {

std::string PB_Serializer::To(bool is_binary, const Message* msg) {
  if (is_binary)
    return msg->SerializeAsString();
  return util::Pb2Json(*msg);
}

bool PB_Serializer::From(bool is_binary, std::string tmp, Message* res) {
  if (is_binary) {
    return res->ParseFromString(tmp);
  }

  util::Status status = util::Json2Pb(std::move(tmp), res);
  DVLOG(1) << "Status: " << status;

  return status.ok();
}

}  // namespace mr3
