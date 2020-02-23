// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/aws/s3.h"
#include "util/aws/aws.h"

namespace util {

S3::S3(const AWS& aws, http::HttpsClientPool* pool) : aws_(aws), pool_(pool) {

}


}  // namespace util
