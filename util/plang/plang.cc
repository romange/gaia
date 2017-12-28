// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/plang/plang.h"

#include "base/logging.h"
#include "strings/util.h"
#include "util/math/mathutil.h"

#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>

using std::string;
using namespace std::placeholders;

namespace plang {

typedef std::pair<const gpb::Message*, const gpb::FieldDescriptor*> MsgDscrPair;
typedef std::vector<std::tuple<uint32, int, MsgDscrPair>> PathState;

static const gpb::Message* AdvanceState(PathState* state) {
  while (!state->empty()) {
    auto& b = state->back();
    int index = ++std::get<1>(b);
    MsgDscrPair& result = std::get<2>(b);
    CHECK(result.second->is_repeated());
    const gpb::Reflection* refl = result.first->GetReflection();
    if (index >= refl->FieldSize(*result.first, result.second)) {
      state->pop_back();
    } else {
      return &refl->GetRepeatedMessage(*result.first, result.second, index);
    }
  }
  return nullptr;
}

static void RetrieveNode(
  const gpb::Message* msg, StringPiece path, std::function<void(const MsgDscrPair&)> cb) {
  MsgDscrPair result(msg, nullptr);
  uint32 start = 0;

  // path index, array index, msg, fd
  PathState state;
  while(result.first != nullptr) {
    size_t next = path.find('.', start);
    StringPiece part = path.subpiece(start, next - start);
    VLOG(2) << "Looking for " << part << " in " << result.first->GetDescriptor()->name()
            << " next= " << next;
    result.second = result.first->GetDescriptor()->FindFieldByName(part.as_string());
    CHECK(result.second != nullptr) << "Could not find field " << part;
    if (next == string::npos) {
      cb(result);
      if ((result.first = AdvanceState(&state)) != nullptr) {
        start = std::get<0>(state.back());
      }
      continue;
    }
    CHECK_EQ(result.second->cpp_type(), gpb::FieldDescriptor::CPPTYPE_MESSAGE) << part
          << " is not a message.";
    const gpb::Reflection* refl = result.first->GetReflection();
    start = next + 1;
    if (result.second->is_repeated()) {
      if (refl->FieldSize(*result.first, result.second) > 0) {
        state.push_back(std::make_tuple(start, 0, result));
        result.first = &refl->GetRepeatedMessage(*result.first, result.second, 0);
      } else {
        if ((result.first = AdvanceState(&state)) != nullptr) {
          start = std::get<0>(state.back());
        }
      }
    } else {
      result.first = &refl->GetMessage(*result.first, result.second);
    }
  }
}

double ExprValue::PromoteToDouble() const {
  switch(type) {
    case CPPTYPE_INT64:
      return val.int_val;
    case CPPTYPE_DOUBLE:
      return val.d_val;
    case CPPTYPE_UINT64:
      return val.uint_val;
    default:
      LOG(FATAL) << "Not supported " << type;
  }
  return 0;
}

bool ExprValue::Equal(const ExprValue& other) const {
  CppType t1 = type, t2 = other.type;
  if (t1 == t2) {
    switch(t1) {
      case CPPTYPE_INT64:
        return val.int_val == other.val.int_val;
      case CPPTYPE_STRING:
        return val.str == other.val.str;
      case CPPTYPE_DOUBLE:
        return MathUtil::AlmostEquals(val.d_val, other.val.d_val);
      case CPPTYPE_ENUM:
        return val.enum_val == other.val.enum_val;
      default:
        LOG(FATAL) << "Not supported " << type;
    }
  }
  if (t1 == CPPTYPE_ENUM) {
    switch(t2) {
      case CPPTYPE_INT64:
        return val.enum_val->number() == other.val.int_val;
      case CPPTYPE_STRING:
        return val.enum_val->name() == other.val.str.as_string();
      default:
        LOG(FATAL) << "Unsupported type for comparing with enum " << t2;
    }
  }
  if (t1 == CPPTYPE_DOUBLE && t2 == CPPTYPE_INT64) {
    return MathUtil::AlmostEquals(val.d_val, double(other.val.int_val));
  }
  if (t2 == CPPTYPE_ENUM || t2 == CPPTYPE_DOUBLE) {
    return other.Equal(*this);
  }

  if (t1 <= 4) { // integer values
    CHECK_LE(other.type, 4);
    CHECK_EQ(0, t1 % 2);
    CHECK_EQ(0, t2 % 2);
    if (t1 != t2) {
      if (t1 == CPPTYPE_INT64) {
        if (val.int_val < 0)
          return false;  // the other value is unsigned so it's different.
        return uint64(val.int_val) == other.val.uint_val;
      }
      DCHECK_EQ(CPPTYPE_INT64, t2);
      if (other.val.int_val < 0)
        return false;
      return uint64(other.val.int_val) == val.uint_val;
    }
  }
  LOG(FATAL) << "Unsupported combination " << t1 << " and " << t2;
  return false;
}

bool ExprValue::Less(const ExprValue& other) const {
  CppType t1 = type, t2 = other.type;
  CHECK_LE(t1, 5);
  CHECK_LE(t2, 5);
  if (t1 != t2) {
    if (t1 == CPPTYPE_DOUBLE || t2 == CPPTYPE_DOUBLE) {
      double d1 = PromoteToDouble();
      double d2 = PromoteToDouble();
      return d1 < d2;
    }
    if (t1 == CPPTYPE_INT64) {
      if (val.int_val < 0)
        return true;  // the other value is unsigned so it's bigger.
      return uint64(val.int_val) < other.val.uint_val;
    }
    DCHECK_EQ(CPPTYPE_INT64, t2);
    if (other.val.int_val <= 0)
      return false;
    return val.uint_val < uint64(other.val.int_val);
  }
  switch(t1) {
    case CPPTYPE_INT64:
      return val.int_val < other.val.int_val;
    case CPPTYPE_UINT64:
      return val.uint_val < other.val.uint_val;
    case CPPTYPE_DOUBLE:
      return val.d_val < other.val.d_val;
    default:
      LOG(FATAL) << "Not supported " << type;
  }
  return false;
}

static void EvalField(Expr::ExprValueCb cb, MsgDscrPair msg_dscr) {
  const gpb::Message* pmsg = msg_dscr.first;
  const gpb::FieldDescriptor* fd = msg_dscr.second;
  const gpb::Reflection* refl = pmsg->GetReflection();

  typedef gpb::FieldDescriptor FD;
  if (fd->is_repeated()) {
    switch(fd->cpp_type()) {
      case FD::CPPTYPE_INT32: {
          const auto& arr = refl->GetRepeatedField<int32>(*pmsg, fd);
          for (int32 val : arr) {
            cb(ExprValue::fromInt(val));
          }
        }
        return;
      case FD::CPPTYPE_UINT32: {
          const auto& arr = refl->GetRepeatedField<uint32>(*pmsg, fd);
          for (uint32 val : arr) {
            cb(ExprValue::fromInt(val));
          }
        }
        return;
      default:
        LOG(FATAL) << "Not supported repeated " << fd->cpp_type_name();
    }
  }
  switch(fd->cpp_type()) {
    case FD::CPPTYPE_INT32:
      cb(ExprValue::fromInt(refl->GetInt32(*pmsg, fd)));
      return;
    case FD::CPPTYPE_UINT32:
      cb(ExprValue::fromUInt(refl->GetUInt32(*pmsg, fd)));
      return;
    case FD::CPPTYPE_INT64:
      cb(ExprValue::fromInt(refl->GetInt64(*pmsg, fd)));
      return;
    case FD::CPPTYPE_UINT64:
      cb(ExprValue::fromUInt(refl->GetUInt64(*pmsg, fd)));
      return;
    case FD::CPPTYPE_STRING: {
      string tmp;
      cb(ExprValue(refl->GetStringReference(*pmsg, fd, &tmp)));
      return;
    }
    case FD::CPPTYPE_FLOAT:
      cb(ExprValue::fromDouble(refl->GetFloat(*pmsg, fd)));
      return;
    case FD::CPPTYPE_DOUBLE:
      cb(ExprValue::fromDouble(refl->GetDouble(*pmsg, fd)));
      return;
    case FD::CPPTYPE_BOOL:
      cb(ExprValue::fromInt(refl->GetBool(*pmsg, fd)));
      return;
    case FD::CPPTYPE_ENUM:
      cb(ExprValue(refl->GetEnum(*pmsg, fd)));
      return;
    default:
      LOG(FATAL) << "Not supported yet " << fd->cpp_type_name();
  }
}

void StringTerm::eval(const gpb::Message& msg, ExprValueCb cb) const {
  if (type_ == CONST) {
    cb(ExprValue(val_));
    return;
  }
  RetrieveNode(&msg, val_, std::bind(&EvalField, cb, _1));
}

template<typename T, typename U> bool IsOneOf(T&& t, const U&& u) {
  return t == u;using namespace std::placeholders;
}

template<typename T, typename U1, typename... U2> bool IsOneOf(T&& t, U1&& u, U2&&... rest) {
  return t == u || IsOneOf(t, rest...);
}

void BinOp::eval(const gpb::Message& msg, ExprValueCb cb) const {
  bool res = false;
  Expr::ExprValueCb local_cb;
  switch (type_) {
    case EQ:
       local_cb = [this, &res, &msg](const ExprValue& val_left) {
               right_->eval(msg, [&val_left, &res](const ExprValue& val_right) {
                            if (val_left.Equal(val_right))
                              res = true;
                            return !res;
                            });
               return !res;
               };
    break;
    case AND:
        local_cb = [this, &res, &msg](const ExprValue& val_left) {
               CHECK_EQ(ExprValue::CPPTYPE_BOOL, val_left.type);
               if (!val_left.val.bool_val) return true;  // continue

               right_->eval(msg, [&res](const ExprValue& val_right) {
                            CHECK_EQ(ExprValue::CPPTYPE_BOOL, val_right.type);
                            if (!val_right.val.bool_val) return true;
                            res = true;
                            return false;
                            });
               return !res;
               };
    break;
    case OR:
        local_cb = [this, &res, &msg](const ExprValue& val_left) {
               CHECK_EQ(ExprValue::CPPTYPE_BOOL, val_left.type);
               if (val_left.val.bool_val) {
                res = true;
                return false;  // continue
               }
               right_->eval(msg, [&res](const ExprValue& val_right) {
                            CHECK_EQ(ExprValue::CPPTYPE_BOOL, val_right.type);
                            if (!val_right.val.bool_val) return true;
                            res = true;
                            return false;
                            });
               return !res;
               };
    break;
    case LT:
        local_cb = [this, &res, &msg](const ExprValue& val_left) {
               right_->eval(msg, [&val_left, &res](const ExprValue& val_right) {
                            if (val_left.Less(val_right))
                              res = true;
                            return !res;
                            });
               return !res;
               };
    break;
    case LE:
        local_cb = [this, &res, &msg](const ExprValue& val_left) {
               right_->eval(msg, [&val_left, &res](const ExprValue& val_right) {
                            if (val_left.Less(val_right) || val_left.Equal(val_right))
                              res = true;
                            return !res;
                            });
               return !res;
               };
        break;
    case NOT:
         local_cb = [this, &res, &msg](const ExprValue& val_left) {
              CHECK_EQ(ExprValue::CPPTYPE_BOOL, val_left.type);
              if (!val_left.val.bool_val) {
                res = true;
              }
              return !res;
            };
    break;
  }
  left_->eval(msg, local_cb);
  cb(ExprValue::fromBool(res));
}

FunctionTerm::FunctionTerm(const std::string& name, ArgList&& lst)
    : name_(name), args_(std::move(lst)) {
  strings::LowerString(&name_);
}

FunctionTerm::~FunctionTerm() {
  for (auto e : args_) delete e;
}

void FunctionTerm::eval(const gpb::Message& msg, ExprValueCb cb) const {
  LOG(FATAL) << "Unknown function";
}

static void IsDefField(Expr::ExprValueCb cb, MsgDscrPair msg_dscr) {
  const gpb::Message* pmsg = msg_dscr.first;
  const gpb::FieldDescriptor* fd = msg_dscr.second;
  const gpb::Reflection* refl = pmsg->GetReflection();
  bool res = fd->is_repeated() ? refl->FieldSize(*pmsg, fd) > 0 : refl->HasField(*pmsg, fd);
  cb(ExprValue::fromBool(res));
}

void IsDefFun::eval(const gpb::Message& msg, ExprValueCb cb) const {
  VLOG(1) << "IsDefFun " << name_;
  RetrieveNode(&msg, name_, std::bind(&IsDefField, cb, _1));
}

bool EvaluateBoolExpr(const Expr& e, const gpb::Message& msg) {
  bool res = false;
  e.eval(msg, [&res](const plang::ExprValue& val) {
          CHECK_EQ(ExprValue::CPPTYPE_BOOL, val.type);
          res = val.val.bool_val;
          return false;
         });
  return res;
}

}  // namespace plang
