// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef _PLANG_H
#define _PLANG_H

#include <functional>
#include <memory>

#include "base/integral_types.h"
#include "strings/stringpiece.h"

namespace google {
namespace protobuf {
class Message;
class EnumValueDescriptor;
}  // namespace protobuf
}  // namespace google

namespace plang {

namespace gpb = ::google::protobuf;

union ExprValueUnion {
  StringPiece str;
  int64 int_val;
  uint64 uint_val;
  bool bool_val;
  double d_val;

  const gpb::EnumValueDescriptor* enum_val;

  ExprValueUnion() : str() {}
};

class ExprValue {
  double PromoteToDouble() const;
public:
  ExprValueUnion val;

  // Copied from descriptor in order not to drag its depedency here.
  enum CppType {
    CPPTYPE_INT32       = 1,
    CPPTYPE_INT64       = 2,
    CPPTYPE_UINT32      = 3,
    CPPTYPE_UINT64      = 4,
    CPPTYPE_DOUBLE      = 5,
    CPPTYPE_FLOAT       = 6,
    CPPTYPE_BOOL        = 7,
    CPPTYPE_ENUM        = 8,
    CPPTYPE_STRING      = 9,
    CPPTYPE_MESSAGE     = 10,

    MAX_CPPTYPE         = 10,
  };

  CppType type;

  explicit ExprValue(CppType t) : type(t) {}
  ExprValue() : type(CPPTYPE_STRING) {}

  static ExprValue fromInt(int64 ival) {
    ExprValue res{CPPTYPE_INT64};
    res.val.int_val = ival;
    return res;
  }

  static ExprValue fromUInt(uint64 val) {
    ExprValue res{CPPTYPE_INT64};
    res.val.uint_val = val;
    return res;
  }

  explicit ExprValue(StringPiece str) : type(CPPTYPE_STRING) {
    val.str = str;
  }

  explicit ExprValue(const gpb::EnumValueDescriptor* eval) : type(CPPTYPE_ENUM) {
    val.enum_val = eval;
  }

  static ExprValue fromBool(bool b) {
    ExprValue res{CPPTYPE_BOOL};
    res.val.bool_val = b;
    return res;
  }

  static ExprValue fromDouble(double d) {
    ExprValue res{CPPTYPE_DOUBLE};
    res.val.d_val = d;
    return res;
  }

  bool Equal(const ExprValue& other) const;
  bool Less(const ExprValue& other) const;
  bool RLike(const ExprValue& other) const;
};

class Expr {
public:
  typedef std::function<bool(const ExprValue&)> ExprValueCb;

  // cb will be called for each value evaluated by Expr until it goes over all values returned by
  // this expression or cb returns false.
  // We need this weird interface because of the repeated fields.
  virtual void eval(const gpb::Message& msg, ExprValueCb cb) const = 0;
  virtual ~Expr() {};
};

typedef std::vector<Expr*> ArgList;

class NumericLiteral : public Expr {
  union {
    int64 signed_val;
    uint64 uval;
    double dval;
  } val_;
  enum class ValType { SINT64, UINT64, DOUBLE };
  ValType val_type_;
public:
  static NumericLiteral Signed(int64 v) {
    NumericLiteral lit;
    lit.val_.signed_val = v;
    lit.val_type_ = ValType::SINT64;
    return lit;
  }

  static NumericLiteral Unsigned(uint64 v) {
    NumericLiteral lit;
    lit.val_.uval = v;
    lit.val_type_ = ValType::UINT64;
    return lit;
  }

  static NumericLiteral Double(double v) {
    NumericLiteral lit;
    lit.val_.dval = v;
    lit.val_type_ = ValType::DOUBLE;
    return lit;
  }

  virtual void eval(const gpb::Message& msg, ExprValueCb cb) const override {
    switch (val_type_) {
    case ValType::UINT64:
      cb(ExprValue::fromUInt(val_.uval));
      break;
    case ValType::SINT64:
      cb(ExprValue::fromInt(val_.signed_val));
      break;
    case ValType::DOUBLE:
      cb(ExprValue::fromDouble(val_.dval));
    }
  }
};

class StringTerm : public Expr {
  std::string val_;
  mutable std::string tmp_;
public:
  enum Type { CONST, VARIABLE};

  StringTerm(const std::string& v, Type t) : val_(v), type_(t) {}

  virtual void eval(const gpb::Message& msg, ExprValueCb cb) const override;
  const std::string& val() const { return val_; }
private:
  Type type_;
};

class BinOp : public Expr {
  std::unique_ptr<Expr> left_;
  std::unique_ptr<Expr> right_;

public:
  enum Type {EQ, AND, OR, LT, LE, NOT, RLIKE};
  BinOp(Type t, Expr* l, Expr* r) : left_(l), right_(r), type_(t) {}

  virtual void eval(const gpb::Message& msg, ExprValueCb cb) const override;
  const Expr& left() const { return *left_.get(); }
  const Expr& right() const { return *right_.get(); }
  Type type() const { return type_; }
private:
  Type type_;
};

class FunctionTerm : public Expr {
  std::string name_;
  ArgList args_;
public:
  FunctionTerm(const std::string& name, ArgList&& lst);
  ~FunctionTerm();

  virtual void eval(const gpb::Message& msg, ExprValueCb cb) const override;
};

class IsDefFun : public Expr {
  std::string name_;
public:
  IsDefFun(const std::string& name) : name_(name) {};
  virtual void eval(const gpb::Message& msg, ExprValueCb cb) const override;
};

bool EvaluateBoolExpr(const Expr& e, const gpb::Message& msg);

}  // namespace plang

#endif  // _PLANG_H
