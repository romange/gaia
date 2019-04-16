// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>

namespace mr3 {

template <typename T> class DoContext;

template <typename FromType, typename Class, typename O>
using EmitMemberFn = void (Class::*)(FromType, DoContext<O>*);

using RawRecord = ::std::string;

typedef std::function<void(RawRecord&& record)> RawSinkCb;

template <typename Handler, typename ToType>
using RawSinkMethodFactory = std::function<RawSinkCb(Handler* handler, DoContext<ToType>* context)>;

}  // namespace mr3
