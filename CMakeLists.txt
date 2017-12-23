cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)

# package information
set(PACKAGE_NAME      "gaia")
set(PACKAGE_VERSION   "")
set(PACKAGE_STRING    "${PACKAGE_NAME} ${PACKAGE_VERSION}")
set(PACKAGE_TARNAME   "${PACKAGE_NAME}-${PACKAGE_VERSION}")
set(PACKAGE_BUGREPORT "TBD")
set(PROJECT_CONTACT romange@gmail.com)

project(${PACKAGE_NAME} CXX)

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
add_subdirectory(abseil-cpp)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_subdirectory(base)
add_subdirectory(util)
add_subdirectory(examples)

