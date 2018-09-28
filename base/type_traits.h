#ifndef BEERI_BASE_TYPE_TRAITS_H_
#define BEERI_BASE_TYPE_TRAITS_H_

#include <type_traits>

namespace base {

// See
// http://stackoverflow.com/questions/32007938/how-to-access-a-possibly-unexisting-type-alias-in-c11
// and http://stackoverflow.com/questions/27687389/how-does-void-t-work
// and
template <typename... Ts>
struct voider {
  using type = void;
};

template <typename... Ts>
using void_t = typename voider<Ts...>::type;

// Bringing is_invokable from c++17.
template <typename F, typename... Args>
struct is_invocable : std::is_constructible<std::function<void(Args...)>,
                                            std::reference_wrapper<std::remove_reference_t<F>>> {};

template <typename R, typename F, typename... Args>
struct is_invocable_r : std::is_constructible<std::function<R(Args...)>,
                                              std::reference_wrapper<std::remove_reference_t<F>>> {
};

}  // namespace base

// Right now these macros are no-ops, and mostly just document the fact
// these types are PODs, for human use.  They may be made more contentful
// later.  The typedef is just to make it legal to put a semicolon after
// these macros.
// #define DECLARE_POD(TypeName) typedef int Dummy_Type_For_DECLARE_POD
#define PROPAGATE_POD_FROM_TEMPLATE_ARGUMENT(TemplateName) \
  typedef int Dummy_Type_For_PROPAGATE_POD_FROM_TEMPLATE_ARGUMENT

#define GENERATE_TYPE_MEMBER_WITH_DEFAULT(Type, member, def_type) \
  template <typename T, typename = void>                          \
  struct Type {                                                   \
    using type = def_type;                                        \
  };                                                              \
                                                                  \
  template <typename T>                                           \
  struct Type<T, ::base::void_t<typename T::member>> {            \
    using type = typename T::member;                              \
  }

// specialized as has_member< T , void > or discarded (sfinae)
#define DEFINE_HAS_MEMBER(name, member) \
  template <typename, typename = void>  \
  struct name : std::false_type {};     \
  template <typename T>                 \
  struct name<T, ::base::void_t<decltype(T::member)>> : std::true_type {}

// Use it like this:
// DEFINE_HAS_SIGNATURE(has_foo, T::foo, void (*)(void));
//
#define DEFINE_HAS_SIGNATURE(TraitsName, funcName, signature)          \
  template <typename U>                                                \
  class TraitsName {                                                   \
    template <typename T, T>                                           \
    struct helper;                                                     \
    template <typename T>                                              \
    static char check(helper<signature, &funcName>*);                  \
    template <typename T>                                              \
    static long check(...);                                            \
                                                                       \
   public:                                                             \
    static constexpr bool value = sizeof(check<U>(0)) == sizeof(char); \
    using type = std::integral_constant<bool, value>;                  \
  }

#define DEFINE_GET_FUNCTION_TRAIT(Name, FuncName, Signature)      \
  template <typename T>                                           \
  class Name {                                                    \
    template <typename U, U>                                      \
    struct helper;                                                \
    template <typename U>                                         \
    static Signature Internal(helper<Signature, &U::FuncName>*) { \
      return &U::FuncName;                                        \
    }                                                             \
    template <typename U>                                         \
    static Signature Internal(...) {                              \
      return nullptr;                                             \
    }                                                             \
                                                                  \
   public:                                                        \
    static Signature Get() {                                      \
      return Internal<T>(0);                                      \
    }                                                             \
  }

#endif  // BEERI_BASE_TYPE_TRAITS_H_
