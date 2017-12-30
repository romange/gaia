// Copyright 2005 Google Inc.
//
// #status: RECOMMENDED
// #category: maps
// #summary: Utility functions for use with map-like containers.
//
// This file provides utility functions for use with STL map-like data
// structures, such as std::map and hash_map. Some functions will also work with
// sets, such as ContainsKey().
//
// The main functions in this file fall into the following categories:
//
// - Find*()
// - Contains*()
// - Insert*()
// - Lookup*()
//
// These functions often have "...OrDie" or "...OrDieNoPrint" variants. These
// variants will crash the process with a CHECK() failure on error, including
// the offending key/data in the log message. The NoPrint variants will not
// include the key/data in the log output under the assumption that it's not a
// printable type.
//
// Most functions are fairly self explanatory from their names, with the
// exception of Find*() vs Lookup*(). The Find functions typically use the map's
// .find() member function to locate and return the map's value type. The
// Lookup*() functions typically use the map's .insert() (yes, insert) member
// function to insert the given value if necessary and returns (usually a
// reference to) the map's value type for the found item.
//
// See the per-function comments for specifics.
//
// There are also a handful of functions for doing other miscellaneous things.
//
// A note on terminology:
//
// Map-like containers are collections of pairs. Like all STL containers they
// contain a few standard typedefs identifying the types of data they contain.
// Given the following map declaration:
//
//   map<string, int> my_map;
//
// the notable typedefs would be as follows:
//
//   - key_type    -- string
//   - value_type  -- pair<const string, int>
//   - mapped_type -- int
//
// Note that the map above contains two types of "values": the key-value pairs
// themselves (value_type) and the values within the key-value pairs
// (mapped_type). A value_type consists of a key_type and a mapped_type.
//
// The documentation below is written for programmers thinking in terms of keys
// and the (mapped_type) values associated with a given key.  For example, the
// statement
//
//   my_map["foo"] = 3;
//
// has a key of "foo" (type: string) with a value of 3 (type: int).
//

#ifndef UTIL_GTL_MAP_UTIL_H_
#define UTIL_GTL_MAP_UTIL_H_

#include <stddef.h>
#include <utility>
#include <vector>
#include <type_traits>
#include "base/logging.h"

using std::pair;

//
// Find*()
//

// Returns a const reference to the value associated with the given key if it
// exists. Crashes otherwise.
//
// This is intended as a replacement for operator[] as an rvalue (for reading)
// when the key is guaranteed to exist.
//
// operator[] for lookup is discouraged for several reasons:
//  * It has a side-effect of inserting missing keys
//  * It is not thread-safe (even when it is not inserting, it can still
//      choose to resize the underlying storage)
//  * It invalidates iterators (when it chooses to resize)
//  * It default constructs a value object even if it doesn't need to
//
// This version assumes the key is printable, and includes it in the fatal log
// message.
template <class Collection>
const typename Collection::value_type::second_type&
FindOrDie(const Collection& collection,
          const typename Collection::value_type::first_type& key) {
  typename Collection::const_iterator it = collection.find(key);
  CHECK(it != collection.end()) << "Map key not found: " << key;
  return it->second;
}

// Same as above, but returns a non-const reference.
template <class Collection>
typename Collection::value_type::second_type&
FindOrDie(Collection& collection,  // NOLINT
          const typename Collection::value_type::first_type& key) {
  typename Collection::iterator it = collection.find(key);
  CHECK(it != collection.end()) << "Map key not found: " << key;
  return it->second;
}

// Returns a const reference to the value associated with the given key if it
// exists, otherwise a const reference to the provided default value is
// returned.
//
template <class Collection>
const typename Collection::value_type::second_type&
FindWithDefault(const Collection& collection,
                const typename Collection::value_type::first_type& key,
                const typename Collection::value_type::second_type& value) {
  typename Collection::const_iterator it = collection.find(key);
  if (it == collection.end()) {
    return value;
  }
  return it->second;
}

template <class Collection>
const typename Collection::value_type::second_type&
FindWithDefault(const Collection& collection,
                const typename Collection::value_type::first_type& key,
                const typename Collection::value_type::second_type&& value) = delete;

template <class Collection>
typename Collection::value_type::second_type
FindValueWithDefault(const Collection& collection,
                     const typename Collection::value_type::first_type& key,
                     typename Collection::value_type::second_type value) {
  typename Collection::const_iterator it = collection.find(key);
  if (it == collection.end()) {
    return value;
  }
  return it->second;
}


// Returns a pointer to the const value associated with the given key if it
// exists, or NULL otherwise.
template <class Collection>
const typename Collection::value_type::second_type*
FindOrNull(const Collection& collection,
           const typename Collection::value_type::first_type& key) {
  typename Collection::const_iterator it = collection.find(key);
  if (it == collection.end()) {
    return 0;
  }
  return &it->second;
}

// Same as above but returns a pointer to the non-const value.
template <class Collection>
typename Collection::value_type::second_type*
FindOrNull(Collection& collection,  // NOLINT
           const typename Collection::value_type::first_type& key) {
  typename Collection::iterator it = collection.find(key);
  if (it == collection.end()) {
    return 0;
  }
  return &it->second;
}

// Returns the pointer value associated with the given key. If none is found,
// NULL is returned. The function is designed to be used with a map of keys to
// pointers.
//
// This function does not distinguish between a missing key and a key mapped
// to a NULL value.
template <class Collection>
typename Collection::value_type::second_type
FindPtrOrNull(const Collection& collection,
              const typename Collection::value_type::first_type& key) {
  typename Collection::const_iterator it = collection.find(key);
  if (it == collection.end()) {
    return typename Collection::value_type::second_type(0);
  }
  return it->second;
}


// Given collection<Key,Value*>, tries to find key and if finds returns its value,
// otherwise allocates new value for that key and returns the new instance.
// second parameter is true if Key was found or false if the new allocation and insertion happened.
template <typename Collection> std::pair<typename Collection::value_type::second_type, bool>
FindPtrOrAllocate(
    Collection* const collection,
    const typename Collection::value_type::first_type& key) {
  using Value = typename Collection::value_type::second_type;

  static_assert(std::is_pointer<Value>::value, "must be pointer");

  auto res = collection->emplace(key, nullptr);
  if (res.second) {
    res.first->second = new typename std::remove_pointer<Value>::type;
  }
  return std::pair<Value, bool>(res.first->second, !res.second);
}



// Same as above, except takes non-const reference to collection.
//
// This function is needed for containers that propagate constness to the
// pointee, such as boost::ptr_map.
template <class Collection>
typename Collection::value_type::second_type
FindPtrOrNull(Collection& collection,  // NOLINT
              const typename Collection::value_type::first_type& key) {
  typename Collection::iterator it = collection.find(key);
  if (it == collection.end()) {
    return typename Collection::value_type::second_type(0);
  }
  return it->second;
}

//
// Contains*()
//

// Returns true iff the given collection contains the given key.
template <class Collection, class Key>
bool ContainsKey(const Collection& collection, const Key& key) {
  typename Collection::const_iterator it = collection.find(key);
  return it != collection.end();
}

// Returns true iff the given collection contains the given key-value pair.
template <class Collection, class Key, class Value>
bool ContainsKeyValuePair(const Collection& collection,
                          const Key& key,
                          const Value& value) {
  typedef typename Collection::const_iterator const_iterator;
  pair<const_iterator, const_iterator> range = collection.equal_range(key);
  for (const_iterator it = range.first; it != range.second; ++it) {
    if (it->second == value) {
      return true;
    }
  }
  return false;
}

//
// Insert*()
//

// Inserts the given key-value pair into the collection. Returns true if the
// given key didn't previously exist. If the given key already existed in the
// map, its value is changed to the given "value" and false is returned.
template <class Collection>
bool InsertOrUpdate(Collection* const collection,
                    const typename Collection::value_type& vt) {
  pair<typename Collection::iterator, bool> ret = collection->insert(vt);
  if (!ret.second) {
    // update
    ret.first->second = vt.second;
    return false;
  }
  return true;
}

// Same as above, except that the key and value are passed separately.
template <class Collection>
bool InsertOrUpdate(Collection* const collection,
                    const typename Collection::value_type::first_type& key,
                    const typename Collection::value_type::second_type& value) {
  return InsertOrUpdate(
      collection, typename Collection::value_type(key, value));
}

// Inserts/updates all the key-value pairs from the range defined by the
// iterators "first" and "last" into the given collection.
template <class Collection, class InputIterator>
void InsertOrUpdateMany(Collection* const collection,
                        InputIterator first, InputIterator last) {
  for (; first != last; ++first) {
    InsertOrUpdate(collection, *first);
  }
}

// Change the value associated with a particular key in a map or hash_map
// of the form map<Key, Value*> which owns the objects pointed to by the
// value pointers.  If there was an existing value for the key, it is deleted.
// True indicates an insert took place, false indicates an update + delete.
template <class Collection>
bool InsertAndDeleteExisting(
    Collection* const collection,
    const typename Collection::value_type::first_type& key,
    const typename Collection::value_type::second_type& value) {
  pair<typename Collection::iterator, bool> ret =
      collection->insert(typename Collection::value_type(key, value));
  if (!ret.second) {
    delete ret.first->second;
    ret.first->second = value;
    return false;
  }
  return true;
}


// Same as above except dies if the key already exists in the collection.
template <class Collection>
void InsertOrDie(Collection* const collection,
                 const typename Collection::value_type& value) {
  CHECK(InsertIfNotPresent(collection, value)) << "duplicate value: " << value;
}

// Same as above except doesn't log the value on error.
template <class Collection>
void InsertOrDieNoPrint(Collection* const collection,
                        const typename Collection::value_type& value) {
  CHECK(InsertIfNotPresent(collection, value)) << "duplicate value.";
}

// Inserts the key-value pair into the collection. Dies if key was already
// present.
template <class Collection>
void InsertOrDie(Collection* const collection,
                 const typename Collection::value_type::first_type& key,
                 const typename Collection::value_type::second_type& data) {
  CHECK(InsertIfNotPresent(collection, key, data))
      << "duplicate key: " << key;
}

// Inserts a new key and default-initialized value. Dies if the key was already
// present. Returns a reference to the value. Example usage:
//
// map<int, SomeProto> m;
// SomeProto& proto = InsertKeyOrDie(&m, 3);
// proto.set_field("foo");
template <class Collection>
typename Collection::value_type::second_type& InsertKeyOrDie(
    Collection* const collection,
    const typename Collection::value_type::first_type& key) {
  typedef typename Collection::value_type value_type;
  pair<typename Collection::iterator, bool> res =
      collection->insert(value_type(key, typename value_type::second_type()));
  CHECK(res.second) << "duplicate key: " << key;
  return res.first->second;
}

//
// Lookup*()
//

// Looks up a given key and value pair in a collection and inserts the key-value
// pair if it's not already present. Returns a reference to the value associated
// with the key.
template <class Collection>
typename Collection::value_type::second_type&
LookupOrInsert(Collection* const collection,
               const typename Collection::value_type& vt) {
  return collection->insert(vt).first->second;
}

// Same as above except the key-value are passed separately.
template <class Collection>
typename Collection::value_type::second_type&
LookupOrInsert(Collection* const collection,
               const typename Collection::value_type::first_type& key,
               const typename Collection::value_type::second_type& value) {
  return LookupOrInsert(
      collection, typename Collection::value_type(key, value));
}

#endif  // UTIL_GTL_MAP_UTIL_H_
