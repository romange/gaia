// Copyright 2019, Hagana 159.  All rights reserved.
// Author: Ori Brostovski (oribrost@gmail.com)
//
#include "mr/impl/freq_map_wrapper.h"

#include "base/logging.h"

namespace mr3 {
namespace detail {

void FreqMapWrapper::CheckType(const std::type_info& t) const {
  CHECK(t == any_.type()) << "Mismatch between " << t.name() << " and " << any_.type().name();
}

void FreqMapWrapper::Add(const FreqMapWrapper& other) {
  ExtraFunctions *ef = ExtraFuncsFromNullablePair(this, &other);
  ef->Add(other, this);
}

FreqMapWrapper::ExtraFunctions*
FreqMapWrapper::ExtraFuncsFromNullablePair(const FreqMapWrapper *f1, const FreqMapWrapper *f2) {
  ExtraFunctions *ef1 = f1->extra_functions_.get(), *ef2 = f2->extra_functions_.get();
  CHECK(ef1 || ef2) << "Can't run on two empty maps";
  CHECK(!ef1 || typeid(*ef1) == typeid(*ef2)) << "Can't run on maps of different types";
  return ef1 ? ef1 : ef2;
}

} // detail
} // mr3
