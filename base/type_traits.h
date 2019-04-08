// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#ifndef BEERI_BASE_TYPE_TRAITS_H_
#define BEERI_BASE_TYPE_TRAITS_H_

#include <functional>
#include <type_traits>

namespace base {

// See
// http://stackoverflow.com/questions/32007938/how-to-access-a-possibly-unexisting-type-alias-in-c11
// and http://stackoverflow.com/questions/27687389/how-does-void-t-work
// and
template <typename... Ts> struct voider { using type = void; };

template <typename... Ts> using void_t = typename voider<Ts...>::type;

#if __cpp_lib_invoke >= 201411
using std::invoke;
using std::is_invocable;
using std::is_invocable_r;
#else

// Bringing is_invokable from c++17.
template <typename F, typename... Args>
struct is_invocable : std::is_constructible<std::function<void(Args...)>,
                                            std::reference_wrapper<std::remove_reference_t<F>>> {};

template <typename R, typename F, typename... Args>
struct is_invocable_r : std::is_constructible<std::function<R(Args...)>,
                                              std::reference_wrapper<std::remove_reference_t<F>>> {
};

template <typename F, typename... Args>
constexpr auto invoke(F&& f, Args&&... args) noexcept(
    noexcept(static_cast<F&&>(f)(static_cast<Args&&>(args)...)))
    -> decltype(static_cast<F&&>(f)(static_cast<Args&&>(args)...)) {
  return static_cast<F&&>(f)(static_cast<Args&&>(args)...);
}

#endif

// based on https://functionalcpp.wordpress.com/2013/08/05/function-traits/
template <typename F> struct DecayedTupleFromParams;

/*template <typename C, typename R, typename Arg> struct DecayedTupleFromParams<R(C::*)(Arg)> {
  typedef typename std::decay<Arg>::type type;
};*/

template <typename C, typename R, typename... Args>
struct DecayedTupleFromParams<R (C::*)(Args...)> {
  typedef std::tuple<typename std::decay<Args>::type...> type;
};

template <typename C, typename R, typename... Args>
struct DecayedTupleFromParams<R (C::*)(Args...) const> {
  typedef std::tuple<typename std::decay<Args>::type...> type;
};

template <typename C>
struct DecayedTupleFromParams : public DecayedTupleFromParams<decltype(&C::operator())> {};

// Remove the first item in a tuple
template <typename T> struct tuple_tail;

template <typename Head, typename... Tail> struct tuple_tail<std::tuple<Head, Tail...>> {
  using type = std::tuple<Tail...>;
};

// std::function
template <typename FunctionT> struct function_traits {
  using arguments = typename tuple_tail<
      typename function_traits<decltype(&FunctionT::operator())>::arguments>::type;

  static constexpr std::size_t arity = std::tuple_size<arguments>::value;

  template <std::size_t N> using argument_type = typename std::tuple_element<N, arguments>::type;

  using return_type = typename function_traits<decltype(&FunctionT::operator())>::return_type;
};

// Free functions
template <typename ReturnTypeT, typename... Args> struct function_traits<ReturnTypeT(Args...)> {
  using arguments = std::tuple<Args...>;

  static constexpr std::size_t arity = std::tuple_size<arguments>::value;

  template <std::size_t N> using argument_type = typename std::tuple_element<N, arguments>::type;

  using return_type = ReturnTypeT;
};

// Function pointers
template <typename ReturnTypeT, typename... Args>
struct function_traits<ReturnTypeT (*)(Args...)> : function_traits<ReturnTypeT(Args...)> {};

// Lambdas
template<typename ClassT, typename ReturnTypeT, typename ... Args>
struct function_traits<ReturnTypeT (ClassT::*)(Args ...) const>
  : function_traits<ReturnTypeT(ClassT &, Args ...)>
{};

template<typename ClassT, typename ReturnTypeT, typename ... Args>
struct function_traits<ReturnTypeT (ClassT::*)(Args ...)>
  : function_traits<ReturnTypeT(ClassT &, Args ...)>
{};

template<typename FunctionT>
struct function_traits<FunctionT &>: function_traits<FunctionT>
{};

template<typename FunctionT>
struct function_traits<FunctionT &&>: function_traits<FunctionT>
{};

}  // namespace base

// Right now these macros are no-ops, and mostly just document the fact
// these types are PODs, for human use.  They may be made more contentful
// later.  The typedef is just to make it legal to put a semicolon after
// these macros.
// #define DECLARE_POD(TypeName) typedef int Dummy_Type_For_DECLARE_POD
#define PROPAGATE_POD_FROM_TEMPLATE_ARGUMENT(TemplateName) \
  typedef int Dummy_Type_For_PROPAGATE_POD_FROM_TEMPLATE_ARGUMENT

#define GENERATE_TYPE_MEMBER_WITH_DEFAULT(Type, member, def_type)                \
  template <typename T, typename = void> struct Type { using type = def_type; }; \
                                                                                 \
  template <typename T> struct Type<T, ::base::void_t<typename T::member>> {     \
    using type = typename T::member;                                             \
  }

// specialized as has_member< T , void > or discarded (sfinae)
#define DEFINE_HAS_MEMBER(name, member)                                  \
  template <typename, typename = void> struct name : std::false_type {}; \
  template <typename T> struct name<T, ::base::void_t<decltype(T::member)>> : std::true_type {}

// Use it like this:
// DEFINE_HAS_SIGNATURE(has_foo, T::foo, void (*)(void));
//
#define DEFINE_HAS_SIGNATURE(TraitsName, funcName, signature)               \
  template <typename U> class TraitsName {                                  \
    template <typename T, T> struct helper;                                 \
    template <typename T> static char check(helper<signature, &funcName>*); \
    template <typename T> static long check(...);                           \
                                                                            \
   public:                                                                  \
    static constexpr bool value = sizeof(check<U>(0)) == sizeof(char);      \
    using type = std::integral_constant<bool, value>;                       \
  }

#define DEFINE_GET_FUNCTION_TRAIT(Name, FuncName, Signature)                            \
  template <typename T> class Name {                                                    \
    template <typename U, U> struct helper;                                             \
    template <typename U> static Signature Internal(helper<Signature, &U::FuncName>*) { \
      return &U::FuncName;                                                              \
    }                                                                                   \
    template <typename U> static Signature Internal(...) { return nullptr; }            \
                                                                                        \
   public:                                                                              \
    static Signature Get() { return Internal<T>(0); }                                   \
  }

#endif  // BEERI_BASE_TYPE_TRAITS_H_
