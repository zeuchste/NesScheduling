#include "LoggerBindings.hpp"
#include "nes-logger-bindings/../spdlog/lib.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <utility>
#if __cplusplus >= 201703L
#include <string_view>
#endif

#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wmissing-declarations"
#pragma GCC diagnostic ignored "-Wshadow"
#ifdef __clang__
#pragma clang diagnostic ignored "-Wdollar-in-identifier-extension"
#endif // __clang__
#endif // __GNUC__

namespace rust {
inline namespace cxxbridge1 {
// #include "rust/cxx.h"

namespace {
template <typename T>
class impl;
} // namespace

class String;

#ifndef CXXBRIDGE1_RUST_STR
#define CXXBRIDGE1_RUST_STR
class Str final {
public:
  Str() noexcept;
  Str(const String &) noexcept;
  Str(const std::string &);
  Str(const char *);
  Str(const char *, std::size_t);

  Str &operator=(const Str &) & noexcept = default;

  explicit operator std::string() const;
#if __cplusplus >= 201703L
  explicit operator std::string_view() const;
#endif

  const char *data() const noexcept;
  std::size_t size() const noexcept;
  std::size_t length() const noexcept;
  bool empty() const noexcept;

  Str(const Str &) noexcept = default;
  ~Str() noexcept = default;

  using iterator = const char *;
  using const_iterator = const char *;
  const_iterator begin() const noexcept;
  const_iterator end() const noexcept;
  const_iterator cbegin() const noexcept;
  const_iterator cend() const noexcept;

  bool operator==(const Str &) const noexcept;
  bool operator!=(const Str &) const noexcept;
  bool operator<(const Str &) const noexcept;
  bool operator<=(const Str &) const noexcept;
  bool operator>(const Str &) const noexcept;
  bool operator>=(const Str &) const noexcept;

  void swap(Str &) noexcept;

private:
  class uninit;
  Str(uninit) noexcept;
  friend impl<Str>;

  std::array<std::uintptr_t, 2> repr;
};
#endif // CXXBRIDGE1_RUST_STR

#ifndef CXXBRIDGE1_IS_COMPLETE
#define CXXBRIDGE1_IS_COMPLETE
namespace detail {
namespace {
template <typename T, typename = std::size_t>
struct is_complete : std::false_type {};
template <typename T>
struct is_complete<T, decltype(sizeof(T))> : std::true_type {};
} // namespace
} // namespace detail
#endif // CXXBRIDGE1_IS_COMPLETE

template <typename T>
union ManuallyDrop {
  T value;
  ManuallyDrop(T &&value) : value(::std::move(value)) {}
  ~ManuallyDrop() {}
};

namespace {
template <typename T, bool = ::rust::detail::is_complete<T>::value>
struct is_destructible : ::std::false_type {};
template <typename T>
struct is_destructible<T, true> : ::std::is_destructible<T> {};
template <typename T>
struct is_destructible<T[], false> : is_destructible<T> {};
template <typename T, bool = ::rust::is_destructible<T>::value>
struct shared_ptr_if_destructible {
  explicit shared_ptr_if_destructible(typename ::std::shared_ptr<T>::element_type *) {}
};
template <typename T>
struct shared_ptr_if_destructible<T, true> : ::std::shared_ptr<T> {
  using ::std::shared_ptr<T>::shared_ptr;
};
} // namespace
} // namespace cxxbridge1
} // namespace rust

enum class Level : ::std::uint8_t;
using SpdLogger = ::SpdLogger;

#ifndef CXXBRIDGE1_ENUM_Level
#define CXXBRIDGE1_ENUM_Level
enum class Level : ::std::uint8_t {
  Trace = 0,
  Debug = 1,
  Info = 2,
  Warn = 3,
  Error = 4,
  Fatal = 5,
};
#endif // CXXBRIDGE1_ENUM_Level

extern "C" {
void cxxbridge1$initialize_logging(::std::shared_ptr<::SpdLogger> *logger) noexcept;

void cxxbridge1$log(::std::shared_ptr<::SpdLogger> const &log, ::Level level, ::rust::Str file, ::std::uint32_t line_number, ::rust::Str message) noexcept {
  void (*log$)(::std::shared_ptr<::SpdLogger> const &, ::Level, ::rust::Str, ::std::uint32_t, ::rust::Str) = ::log;
  log$(log, level, file, line_number, message);
}

void cxxbridge1$mdc_insert(::rust::Str key, ::rust::Str message) noexcept {
  void (*mdc_insert$)(::rust::Str, ::rust::Str) = ::mdc_insert;
  mdc_insert$(key, message);
}

void cxxbridge1$mdc_remove(::rust::Str key) noexcept {
  void (*mdc_remove$)(::rust::Str) = ::mdc_remove;
  mdc_remove$(key);
}
} // extern "C"

void initialize_logging(::std::shared_ptr<::SpdLogger> logger) noexcept {
  ::rust::ManuallyDrop<::std::shared_ptr<::SpdLogger>> logger$(::std::move(logger));
  cxxbridge1$initialize_logging(&logger$.value);
}

extern "C" {
static_assert(sizeof(::std::shared_ptr<::SpdLogger>) == 2 * sizeof(void *), "");
static_assert(alignof(::std::shared_ptr<::SpdLogger>) == alignof(void *), "");
void cxxbridge1$shared_ptr$SpdLogger$null(::std::shared_ptr<::SpdLogger> *ptr) noexcept {
  ::new (ptr) ::std::shared_ptr<::SpdLogger>();
}
bool cxxbridge1$shared_ptr$SpdLogger$raw(::std::shared_ptr<::SpdLogger> *ptr, ::std::shared_ptr<::SpdLogger>::element_type *raw) noexcept {
  ::new (ptr) ::rust::shared_ptr_if_destructible<::SpdLogger>(raw);
  return ::rust::is_destructible<::SpdLogger>::value;
}
void cxxbridge1$shared_ptr$SpdLogger$clone(::std::shared_ptr<::SpdLogger> const &self, ::std::shared_ptr<::SpdLogger> *ptr) noexcept {
  ::new (ptr) ::std::shared_ptr<::SpdLogger>(self);
}
::std::shared_ptr<::SpdLogger>::element_type const *cxxbridge1$shared_ptr$SpdLogger$get(::std::shared_ptr<::SpdLogger> const &self) noexcept {
  return self.get();
}
void cxxbridge1$shared_ptr$SpdLogger$drop(::std::shared_ptr<::SpdLogger> *self) noexcept {
  self->~shared_ptr();
}
} // extern "C"
