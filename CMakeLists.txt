cmake_minimum_required(VERSION 3.4)

# package information
set(PACKAGE_NAME      "gaia")
set(PACKAGE_VERSION   "")
set(PACKAGE_STRING    "${PACKAGE_NAME} ${PACKAGE_VERSION}")
set(PACKAGE_TARNAME   "${PACKAGE_NAME}-${PACKAGE_VERSION}")
set(PACKAGE_BUGREPORT "TBD")
set(PROJECT_CONTACT romange@gmail.com)

project(${PACKAGE_NAME})

option (ONLY_THIRD_PARTY "Build third party only")

# Check target architecture
if (NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
  message(FATAL_ERROR "Gaia requires a 64bit target architecture.")
endif()

if(NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
  message(FATAL_ERROR "Requires running on linux, found ${CMAKE_SYSTEM_NAME} instead")
endif()

Message("PROJECT_BINARY_DIR ${PROJECT_BINARY_DIR} GENERATOR ${CMAKE_GENERATOR}")

get_directory_property(HAS_PARENT PARENT_DIRECTORY)

if (NOT HAS_PARENT)
  list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

  include(third_party)
  include(internal)
endif ()


set(ABSL_CCTZ_TARGET TRDP::cctz)
set(BUILD_TESTING OFF)
set(CCTZ_INCLUDE_DIRS ${CCTZ_INCLUDE_DIR})
set(GOLD_CXX_FLAGS ${CMAKE_CXX_FLAGS})

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

if (NOT ONLY_THIRD_PARTY)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare -Wno-missing-field-initializers")
  add_subdirectory(abseil-cpp)
  set(CMAKE_CXX_FLAGS "${GOLD_CXX_FLAGS}")

  add_subdirectory(base)
  add_subdirectory(strings)
  add_subdirectory(util)
  add_subdirectory(file)
  add_subdirectory(examples)
endif()
