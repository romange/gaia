// Copyright 2019, Hagana 159.  All rights reserved.
// Author: Ori Brostovski (oribrost@gmail.com)
//
#pragma once

#include "absl/container/flat_hash_map.h"
#include "absl/types/any.h"

template<typename T> using FrequencyMap = absl::flat_hash_map<T, size_t>;

namespace mr3 {
namespace detail {

class FreqMapWrapper {
public:
  template <class T>
  FreqMapWrapper(FrequencyMap<T>&& m) : any_(std::move(m)),
                                        extra_functions_(new ExtraFunctionsImpl<T>) {}

  FreqMapWrapper() {}

  bool has_value() const { return any_.has_value(); }
  std::type_info type() const;
  void Add(const FreqMapWrapper& other);

  template <class T> FrequencyMap<T>& Cast() {
    CheckType(typeid(FrequencyMap<T>));
    return *absl::any_cast<FrequencyMap<T>>(&any_);
  }
  template <class T> const FrequencyMap<T>& Cast() const {
    CheckType(typeid(FrequencyMap<T>));
    return *absl::any_cast<FrequencyMap<T>>(&any_);
  }

private:
  class ExtraFunctions {
  public:
    virtual void Add(const FreqMapWrapper& other, FreqMapWrapper *that) = 0;
    virtual ~ExtraFunctions() {}
  };

  template <class T>
  class ExtraFunctionsImpl : public ExtraFunctions {
  public:
    void Add(const FreqMapWrapper& other, FreqMapWrapper *that) override {
      if (!other.has_value()) // other is empty, so we don't need to add anything.
        return;
      if (!that->has_value()) // that is empty, so before we fill it, we create it.
        that->any_ = FrequencyMap<T>();
      auto& casted_that = that->Cast<T>();
      for (auto& map_value : other.Cast<T>())
        casted_that[map_value.first] += map_value.second;
    }
  };

  static ExtraFunctions *ExtraFuncsFromNullablePair(const FreqMapWrapper*, const FreqMapWrapper*);
  void CheckType(const std::type_info& t) const;

  absl::any any_;
  std::shared_ptr<ExtraFunctions> extra_functions_;
};

} // detail
} // mr3
