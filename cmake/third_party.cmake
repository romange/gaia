set(THIRD_PARTY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party")

SET_DIRECTORY_PROPERTIES(PROPERTIES EP_PREFIX ${THIRD_PARTY_DIR})

Include(ExternalProject)

set(THIRD_PARTY_LIB_DIR "${THIRD_PARTY_DIR}/libs")
set(THIRD_PARTY_CXX_FLAGS "-std=c++11 -O3 -DNDEBUG -fPIC")

function(add_third_party name)
  set(options SHARED)
  set(oneValueArgs CMAKE_PASS_FLAGS INSTALL_OVERRIDE LIB)

  CMAKE_PARSE_ARGUMENTS(parsed "${options}" "${oneValueArgs}" "" ${ARGN})
  set(BUILD_OPTIONS "-j4")

  if (parsed_CMAKE_PASS_FLAGS)
    string(REPLACE " " ";" list_CMAKE_ARGS ${parsed_CMAKE_PASS_FLAGS})
  endif()

  set(INSTALL_CMD "make;install")
  if (parsed_INSTALL_OVERRIDE)
    set(INSTALL_CMD ${parsed_INSTALL_OVERRIDE})
  endif()

  set(_DIR ${THIRD_PARTY_DIR}/${name})
  set(_IROOT ${THIRD_PARTY_LIB_DIR}/${name})
  set(LIB_PREFIX "${_IROOT}/lib/lib${name}.")

  if (parsed_LIB)
    if (${parsed_LIB} MATCHES ".*\.so$")
      set(LIB_TYPE SHARED)
    elseif (${parsed_LIB} MATCHES ".*\.a$")
      set(LIB_TYPE STATIC)
    else()
      MESSAGE(FATAL_ERROR "Unrecognized lib ${parsed_LIB}")
    endif()
    set(LIB_FILE "${_IROOT}/lib/${parsed_LIB}")
  elseif(parsed_SHARED)
    set(LIB_TYPE SHARED)
    STRING(CONCAT LIB_FILE "${LIB_PREFIX}" "so")
  else()
    set(LIB_TYPE STATIC)
    STRING(CONCAT LIB_FILE "${LIB_PREFIX}" "a")
  endif()

  ExternalProject_Add(${name}_project
    DOWNLOAD_DIR ${_DIR}
    SOURCE_DIR ${_DIR}
    INSTALL_DIR ${_IROOT}
    UPDATE_COMMAND ""

    BUILD_COMMAND make ${BUILD_OPTIONS}

    INSTALL_COMMAND ${INSTALL_CMD}

    # Wrap download, configure and build steps in a script to log output
    LOG_INSTALL ON
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON

    CMAKE_GENERATOR "Unix Makefiles"
    BUILD_BYPRODUCTS ${LIB_FILE}

    # we need those CMAKE_ARGS for cmake based 3rd party projects.
    CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY:PATH=${_IROOT}
        -DCMAKE_LIBRARY_OUTPUT_DIRECTORY:PATH=${_IROOT}
        -DCMAKE_BUILD_TYPE:STRING=Release
        -DCMAKE_C_FLAGS:STRING=-O3 -DCMAKE_CXX_FLAGS=${THIRD_PARTY_CXX_FLAGS}
        -DCMAKE_INSTALL_PREFIX:PATH=${_IROOT}
        ${list_CMAKE_ARGS}
    ${parsed_UNPARSED_ARGUMENTS}
  )

  string(TOUPPER ${name} uname)
  file(MAKE_DIRECTORY ${_IROOT}/include)

  set("${uname}_INCLUDE_DIR" "${_IROOT}/include" PARENT_SCOPE)
  set("${uname}_LIB_DIR" "${_IROOT}/lib" PARENT_SCOPE)

  add_library(TRDP::${name} ${LIB_TYPE} IMPORTED)
  add_dependencies(TRDP::${name} ${name}_project)
  set_target_properties(TRDP::${name} PROPERTIES IMPORTED_LOCATION ${LIB_FILE}
                        INTERFACE_INCLUDE_DIRECTORIES ${_IROOT}/include)
endfunction()

# We need gflags as shared library because glog is shared and it uses it too.
# Gflags can not be duplicated inside executable due to its module initialization logic.
add_third_party(
  gflags
  GIT_REPOSITORY https://github.com/gflags/gflags.git
  GIT_TAG v2.2.1
  CMAKE_PASS_FLAGS "-DBUILD_TESTING=OFF -DBUILD_SHARED_LIBS=ON \
                    -DBUILD_STATIC_LIBS=OFF -DBUILD_gflags_nothreads_LIB=OFF"
  SHARED
)


add_third_party(
  glog
  DEPENDS gflags_project
  GIT_REPOSITORY https://github.com/google/glog.git
  GIT_TAG v0.3.4
  PATCH_COMMAND autoreconf --force --install  # needed to refresh toolchain

  CONFIGURE_COMMAND <SOURCE_DIR>/configure
       #--disable-rtti we can not use rtti because of the fucking thrift.
       --with-gflags=${THIRD_PARTY_LIB_DIR}/gflags
       --enable-frame-pointers
       --prefix=${THIRD_PARTY_LIB_DIR}/glog
       --enable-static=yes --enable-shared=no
       CXXFLAGS=${THIRD_PARTY_CXX_FLAGS}
)


add_third_party(
  gtest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.8.0
)

add_third_party(
  benchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG f662e8be5bc9d40640e10b72092780b401612bf2
)

add_third_party(
  gperf
  GIT_REPOSITORY https://github.com/gperftools/gperftools.git
  GIT_TAG gperftools-2.6.3
  PATCH_COMMAND ./autogen.sh
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --enable-frame-pointers --enable-static=no
                    --prefix=${THIRD_PARTY_LIB_DIR}/gperf
  LIB libtcmalloc_and_profiler.so
)

set_property(TARGET TRDP::glog APPEND PROPERTY
             INTERFACE_INCLUDE_DIRECTORIES ${GFLAGS_INCLUDE_DIR})

set_property(TARGET TRDP::gtest APPEND PROPERTY
             IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})

add_library(fast_malloc SHARED IMPORTED)
add_dependencies(fast_malloc gperf_project)
set_target_properties(fast_malloc PROPERTIES IMPORTED_LOCATION
                      ${GPERF_LIB_DIR}/libtcmalloc_and_profiler.so)

link_libraries(unwind ${CMAKE_THREAD_LIBS_INIT})
