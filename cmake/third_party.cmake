set(THIRD_PARTY_DIR "${CMAKE_CURRENT_BINARY_DIR}/third_party")

SET_DIRECTORY_PROPERTIES(PROPERTIES EP_PREFIX ${THIRD_PARTY_DIR})

Include(ExternalProject)

set(THIRD_PARTY_LIB_DIR "${THIRD_PARTY_DIR}/libs")
file(MAKE_DIRECTORY ${THIRD_PARTY_LIB_DIR})
file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${THIRD_PARTY_LIB_DIR} "${CMAKE_CURRENT_SOURCE_DIR}/third_party/libs")

set(THIRD_PARTY_CXX_FLAGS "-std=c++14 -O3 -DNDEBUG -fPIC -DGOOGLE_PROTOBUF_NO_RTTI")

find_package(Threads REQUIRED)
find_library (UNWIND_LIBRARY NAMES unwind DOC "unwind library")
mark_as_advanced (UNWIND_LIBRARY)  ## Hides this variable from GUI.

if (NOT UNWIND_LIBRARY)
  Message(FATAL_ERROR  "libunwind8-dev is not installed but required for better glog stacktraces")
endif ()


function(add_third_party name)
  set(options SHARED)
  set(oneValueArgs CMAKE_PASS_FLAGS)
  set(multiValArgs BUILD_COMMAND INSTALL_COMMAND LIB)
  CMAKE_PARSE_ARGUMENTS(parsed "${options}" "${oneValueArgs}" "${multiValArgs}" ${ARGN})

  if (parsed_CMAKE_PASS_FLAGS)
    string(REPLACE " " ";" piped_CMAKE_ARGS ${parsed_CMAKE_PASS_FLAGS})
  endif()

  if (NOT parsed_INSTALL_COMMAND)
    set(parsed_INSTALL_COMMAND make install)
  endif()

  if (NOT parsed_BUILD_COMMAND)
    set(parsed_BUILD_COMMAND make -j4)
  endif()

  set(_DIR ${THIRD_PARTY_DIR}/${name})
  set(INSTALL_ROOT ${THIRD_PARTY_LIB_DIR}/${name})

  if (parsed_LIB)
    set(LIB_FILES "")

    foreach (_file ${parsed_LIB})
      LIST(APPEND LIB_FILES "${INSTALL_ROOT}/lib/${_file}")
      if (${_file} MATCHES ".*\.so$")
        set(LIB_TYPE SHARED)
      elseif (${_file} MATCHES ".*\.a$")
        set(LIB_TYPE STATIC)
      elseif("${_file}" STREQUAL "none")
        set(LIB_FILES "")
      else()
        MESSAGE(FATAL_ERROR "Unrecognized lib ${_file}")
      endif()
    endforeach(_file)
  else()
    set(LIB_PREFIX "${INSTALL_ROOT}/lib/lib${name}.")

    if(parsed_SHARED)
      set(LIB_TYPE SHARED)
      STRING(CONCAT LIB_FILES "${LIB_PREFIX}" "so")
    else()
      set(LIB_TYPE STATIC)
      STRING(CONCAT LIB_FILES "${LIB_PREFIX}" "a")
    endif(parsed_SHARED)
  endif()

  ExternalProject_Add(${name}_project
    DOWNLOAD_DIR ${_DIR}
    SOURCE_DIR ${_DIR}
    INSTALL_DIR ${INSTALL_ROOT}
    UPDATE_COMMAND ""

    BUILD_COMMAND ${parsed_BUILD_COMMAND}

    INSTALL_COMMAND ${parsed_INSTALL_COMMAND}

    # Wrap download, configure and build steps in a script to log output
    LOG_INSTALL ON
    LOG_DOWNLOAD ON
    LOG_CONFIGURE ON
    LOG_BUILD ON

    CMAKE_GENERATOR "Unix Makefiles"
    BUILD_BYPRODUCTS ${LIB_FILES}
    LIST_SEPARATOR | # Use the alternate list separator

    # we need those CMAKE_ARGS for cmake based 3rd party projects.
    CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY:PATH=${INSTALL_ROOT}
        -DCMAKE_LIBRARY_OUTPUT_DIRECTORY:PATH=${INSTALL_ROOT}
        -DCMAKE_BUILD_TYPE:STRING=Release
        -DBUILD_TESTING=OFF
        -DCMAKE_C_FLAGS:STRING=-O3 -DCMAKE_CXX_FLAGS=${THIRD_PARTY_CXX_FLAGS}
        -DCMAKE_INSTALL_PREFIX:PATH=${INSTALL_ROOT}
        ${piped_CMAKE_ARGS}
    ${parsed_UNPARSED_ARGUMENTS}
  )

  string(TOUPPER ${name} uname)
  file(MAKE_DIRECTORY ${INSTALL_ROOT}/include)

  set("${uname}_INCLUDE_DIR" "${INSTALL_ROOT}/include" PARENT_SCOPE)
  if (LIB_TYPE)
    set("${uname}_LIB_DIR" "${INSTALL_ROOT}/lib" PARENT_SCOPE)
    list(LENGTH LIB_FILES LIB_LEN)
    if (${LIB_LEN} GREATER 1)
      foreach (_file ${LIB_FILES})
        get_filename_component(base_name ${_file} NAME_WE)
        STRING(REGEX REPLACE "^lib" "" tname ${base_name})

        add_library(TRDP::${tname} ${LIB_TYPE} IMPORTED)
        add_dependencies(TRDP::${tname} ${name}_project)
        set_target_properties(TRDP::${tname} PROPERTIES IMPORTED_LOCATION ${_file}
                              INTERFACE_INCLUDE_DIRECTORIES ${INSTALL_ROOT}/include)
      endforeach(_file)
    else()
        add_library(TRDP::${name} ${LIB_TYPE} IMPORTED)
        add_dependencies(TRDP::${name} ${name}_project)
        set_target_properties(TRDP::${name} PROPERTIES IMPORTED_LOCATION ${LIB_FILES}
                              INTERFACE_INCLUDE_DIRECTORIES ${INSTALL_ROOT}/include)
    endif()
  endif()
endfunction()

# We need gflags as shared library because glog is shared and it uses it too.
# Gflags can not be duplicated inside executable due to its module initialization logic.
add_third_party(
  gflags
  GIT_REPOSITORY https://github.com/gflags/gflags.git
  GIT_TAG v2.2.1
  CMAKE_PASS_FLAGS "-DBUILD_SHARED_LIBS=ON \
                    -DBUILD_STATIC_LIBS=OFF -DBUILD_gflags_nothreads_LIB=OFF"
  SHARED
)


add_third_party(
  glog
  DEPENDS gflags_project
  GIT_REPOSITORY https://github.com/romange/glog.git
  GIT_TAG Prod
)


add_third_party(
  gtest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.8.0
  LIB libgtest.a libgmock.a
)

add_third_party(
  benchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG 505be96
  CMAKE_PASS_FLAGS "-DBENCHMARK_ENABLE_GTEST_TESTS=OFF -DBENCHMARK_ENABLE_TESTING=OFF"
)

add_third_party(
  gperf
  GIT_REPOSITORY https://github.com/gperftools/gperftools.git
  GIT_TAG gperftools-2.7
  PATCH_COMMAND ./autogen.sh
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --enable-frame-pointers --enable-static=no
                    --enable-libunwind
                    --prefix=${THIRD_PARTY_LIB_DIR}/gperf
  LIB libtcmalloc_and_profiler.so
)

# abseil has cctz support built-in
# set(CCTZ_DIR ${THIRD_PARTY_LIB_DIR}/cctz)
# add_third_party(cctz
#   GIT_REPOSITORY https://github.com/google/cctz.git
#   GIT_TAG v2.1
#   CONFIGURE_COMMAND true # 'true' is bash's NOP
#   BUILD_COMMAND make -j4 classic
#   INSTALL_COMMAND make install -e PREFIX=${CCTZ_DIR}
#   BUILD_IN_SOURCE 1
# )

#Protobuf project
set(PROTOBUF_DIR ${THIRD_PARTY_LIB_DIR}/protobuf)
set(PROTOC ${PROTOBUF_DIR}/bin/protoc)
add_third_party(
    protobuf
    GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
    GIT_TAG v3.6.1
    PATCH_COMMAND ./autogen.sh

    CONFIGURE_COMMAND <SOURCE_DIR>/configure --with-zlib  --with-tests=no
        "CXXFLAGS=${THIRD_PARTY_CXX_FLAGS}"  --prefix=${PROTOBUF_DIR}
    COMMAND make clean

    LIB libprotobuf.so libprotoc.so
)

file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/proto_python_setup.cmd"
     "PROTOC=${PROTOC} python setup.py build\n"
     "PYTHONPATH=${THIRD_PARTY_LIB_DIR}/lib/python/ python setup.py install "
     "--home=${THIRD_PARTY_LIB_DIR}\n"
     "cd `mktemp -d`\n"
     "mkdir google\n"
     "echo > google/__init__.py\n"
     "zip ${THIRD_PARTY_LIB_DIR}/lib/python/protobuf-3.0.0b2-py2.7.egg google/__init__.py"
     )

ExternalProject_Add_Step(protobuf_project install_python
  DEPENDEES install
  WORKING_DIRECTORY ${THIRD_PARTY_DIR}/protobuf/python
  COMMAND mkdir -p ${THIRD_PARTY_LIB_DIR}/lib/python
  COMMAND bash ${CMAKE_CURRENT_BINARY_DIR}/proto_python_setup.cmd
  LOG 1
)

add_third_party(pmr
  GIT_REPOSITORY https://github.com/romange/pmr.git
)

set(SPARSE_HASH_DIR ${THIRD_PARTY_LIB_DIR}/sparsehash)
add_third_party(
  sparsehash
  GIT_REPOSITORY https://github.com/romange/sparsehash.git
  CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=${SPARSE_HASH_DIR} CXXFLAGS=${THIRD_PARTY_CXX_FLAGS}
  LIB "none"
)
set(SPARSE_HASH_INCLUDE_DIR ${SPARSE_HASH_DIR}/include)


add_third_party(
  rapidjson
  GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
  # GIT_SUBMODULES third_party/foo
  CMAKE_PASS_FLAGS "-DRAPIDJSON_BUILD_TESTS=OFF -DRAPIDJSON_BUILD_EXAMPLES=OFF \
                    -DRAPIDJSON_BUILD_DOC=OFF"
  LIB "none"
)
set(RAPIDJSON_INCLUDE_DIR ${THIRD_PARTY_LIB_DIR}/rapidjson/include)

add_third_party(
  xxhash
  GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
  GIT_TAG v0.6.4
  SOURCE_SUBDIR cmake_unofficial
  CMAKE_PASS_FLAGS "-DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_SHARED_LIBS=OFF"
)

set(LZ4_DIR ${THIRD_PARTY_LIB_DIR}/lz4)
add_third_party(lz4
  GIT_REPOSITORY https://github.com/lz4/lz4.git
  GIT_TAG v1.8.3
  BUILD_IN_SOURCE 1
  UPDATE_COMMAND ""
  SOURCE_SUBDIR contrib/cmake_unofficial
  
  CMAKE_PASS_FLAGS "-DBUILD_SHARED_LIBS=OFF"
)

add_third_party(
  dconv
  GIT_REPOSITORY https://github.com/google/double-conversion.git
  GIT_TAG v3.0.0
  LIB libdouble-conversion.a
)

set(Boost_USE_MULTITHREADED ON)
SET(Boost_NO_SYSTEM_PATHS ON)

set(BOOST_ROOT /usr/local)
find_package(Boost 1.67.0 QUIET COMPONENTS coroutine fiber context system thread)
if (NOT Boost_FOUND)
  set(BOOST_ROOT /opt/boost)
  find_package(Boost 1.67.0 REQUIRED COMPONENTS coroutine fiber context system thread)
endif()

set(LDFOLLY "-L${Boost_LIBRARY_DIR} -L${GFLAGS_LIB_DIR} -L${GLOG_LIB_DIR} -L${DCONV_LIB_DIR} -Wl,-rpath,${Boost_LIBRARY_DIR} -Wl,-rpath,${GFLAGS_LIB_DIR}")
set(CXXFOLLY "-g  -I${GFLAGS_INCLUDE_DIR} -I${GLOG_INCLUDE_DIR} -I${DCONV_INCLUDE_DIR} -I${GTEST_INCLUDE_DIR}")
add_third_party(folly
  DEPENDS gflags_project glog_project dconv_project
  GIT_REPOSITORY https://github.com/facebook/folly.git
  GIT_TAG v2017.12.11.00
  PATCH_COMMAND autoreconf <SOURCE_DIR>/folly/ -ivf
  CONFIGURE_COMMAND echo "hi handsome"
  BUILD_COMMAND make -C folly -j4 install
  INSTALL_COMMAND echo "bye handsome"
  BUILD_IN_SOURCE 1
)

ExternalProject_Add_Step(folly_project config
  DEPENDEES configure
  DEPENDERS build
  WORKING_DIRECTORY ${THIRD_PARTY_DIR}/folly/folly
  COMMAND ./configure --enable-shared=no --with-boost=${BOOST_ROOT}
                      --prefix=${THIRD_PARTY_LIB_DIR}/folly LDFLAGS=${LDFOLLY}
                    CXXFLAGS=${CXXFOLLY} "LIBS=-lpthread -lunwind"

  # Disable sanitization code in library includes.
  COMMAND sed -i "s/__SANITIZE_ADDRESS__/__SANITIZE_ADDRESS_DISABLED/" CPortability.h
  LOG 1
)

set(ZSTD_DIR ${THIRD_PARTY_LIB_DIR}/zstd)
add_third_party(zstd
  GIT_REPOSITORY https://github.com/facebook/zstd.git
  GIT_TAG v1.3.3
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND echo "foo"

  # for debug: "CFLAGS=-fPIC -O0 -ggdb"
  BUILD_COMMAND make -e "CFLAGS=-fPIC -O3" -j4
  INSTALL_COMMAND make install -e PREFIX=${ZSTD_DIR}
)

add_third_party(blosc
  GIT_REPOSITORY https://github.com/romange/c-blosc.git
  # GIT_TAG v1.12.1
  CMAKE_PASS_FLAGS "-DBUILD_TESTS=OFF  -DBUILD_BENCHMARKS=OFF -DDEACTIVATE_SNAPPY=ON -DDEACTIVATE_ZSTD=ON"
)

add_third_party(
    nanoflann
    GIT_REPOSITORY https://github.com/jlblancoc/nanoflann.git
    GIT_TAG v1.2.3
)

add_third_party(snappy
  GIT_REPOSITORY https://github.com/google/snappy.git
  GIT_TAG 1.1.7
)


set(DYNASM_DIR ${THIRD_PARTY_LIB_DIR}/dynasm)
set(DYNASM_COMPILER ${DYNASM_DIR}/bin/minilua)
set(DYNASM_INCLUDE_DIR ${DYNASM_DIR}/include)

add_third_party(dynasm
  URL http://luajit.org/download/LuaJIT-2.1.0-beta3.tar.gz
  PATCH_COMMAND mkdir -p ${DYNASM_INCLUDE_DIR} ${DYNASM_DIR}/bin
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND true
  BUILD_COMMAND gcc -o ${DYNASM_COMPILER} -O3 src/host/minilua.c -lm
  LIB "none"
  INSTALL_COMMAND sh -c "test -L ${DYNASM_INCLUDE_DIR}/dynasm || ln -s ${THIRD_PARTY_DIR}/dynasm/dynasm -t ${DYNASM_INCLUDE_DIR}/"
)

add_third_party(s2geometry
  GIT_REPOSITORY https://github.com/romange/s2geometry.git
  DEPENDS glog_project
  CMAKE_PASS_FLAGS "-DWITH_GFLAGS=ON  -DWITH_GLOG=ON -DCMAKE_PREFIX_PATH=${GLOG_LIB_DIR}/cmake/glog/|${GFLAGS_LIB_DIR}/cmake/gflags"
  LIB "libs2.so"
)


set_property(TARGET TRDP::glog APPEND PROPERTY
             INTERFACE_INCLUDE_DIRECTORIES ${GFLAGS_INCLUDE_DIR}
             )

set_property(TARGET TRDP::glog APPEND PROPERTY
             IMPORTED_LINK_INTERFACE_LIBRARIES unwind)

set_property(TARGET TRDP::folly APPEND PROPERTY
             IMPORTED_LINK_INTERFACE_LIBRARIES event TRDP::dconv ${BOOST_ROOT}/lib/libboost_context.so)

set_property(TARGET TRDP::gtest APPEND PROPERTY
             IMPORTED_LINK_INTERFACE_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})

add_library(fast_malloc SHARED IMPORTED)
add_dependencies(fast_malloc gperf_project)
set_target_properties(fast_malloc PROPERTIES IMPORTED_LOCATION
                      ${GPERF_LIB_DIR}/libtcmalloc_and_profiler.so
                      IMPORTED_LINK_INTERFACE_LIBRARIES unwind
                      INTERFACE_INCLUDE_DIRECTORIES ${GPERF_INCLUDE_DIR}
                      )

link_libraries(${CMAKE_THREAD_LIBS_INIT})
include_directories(${SPARSE_HASH_INCLUDE_DIR} ${Boost_INCLUDE_DIR} ${RAPIDJSON_INCLUDE_DIR})
