// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "mr/mr_pb.h"

namespace mr3 {

std::string PB_Serializer::To(bool is_binary, const Message* msg) {
   return msg->SerializeAsString();
}

bool PB_Serializer::From(bool is_binary, const std::string& tmp, Message* res) {
  return res->ParseFromString(tmp);
}

}  // namespace mr3
