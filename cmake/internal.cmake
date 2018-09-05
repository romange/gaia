# Copyright 2013, Beeri 15.  All rights reserved.
# Author: Roman Gershman (romange@gmail.com)
#

include(CTest)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
enable_language(CXX C)

# Check target architecture
if (NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
  message(FATAL_ERROR "Gaia requires a 64bit target architecture.")
endif()

if(NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
  message(FATAL_ERROR "Requires running on linux, found ${CMAKE_SYSTEM_NAME} instead")
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

include(CheckCXXCompilerFlag)

CHECK_CXX_COMPILER_FLAG("-std=c++14" COMPILER_SUPPORTS_CXX14)

if(NOT COMPILER_SUPPORTS_CXX14)
    message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++14 support. Please use a different C++ compiler.")
endif()

message(STATUS "Compiler ${CMAKE_CXX_COMPILER}, version: ${CMAKE_CXX_COMPILER_VERSION}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

# ---[ Color diagnostics
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcolor-diagnostics -Wno-inconsistent-missing-override")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-local-typedef")
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL 4.9 OR CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 4.9)
      set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=auto")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-color=always")
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fsanitize=undefined -fno-sanitize=vptr")
  endif()
endif()

# can not set -Wshadow  due to numerous warnings in protobuf compilation.
# mcx16 to support double word CAS.
# Protobuf needs run-time information so no -fno-rtti switch.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -march=broadwell -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc ")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-realloc -fno-builtin-free")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -Wno-unused-parameter")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-strict-aliasing -DGOOGLE_PROTOBUF_NO_RTTI")

# Need -fPIC in order to link against shared libraries. For example when creating python modules.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-result")

set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -gdwarf-4")

IF(CMAKE_BUILD_TYPE STREQUAL "Debug")
  MESSAGE (CXX_FLAGS " ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG}")
ELSEIF(CMAKE_BUILD_TYPE STREQUAL "Release")
  MESSAGE (CXX_FLAGS " ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE}")
ELSE()
  MESSAGE(FATAL_ERROR "Unsupported build type '${CMAKE_BUILD_TYPE}'")
ENDIF()

set(ROOT_GEN_DIR ${CMAKE_SOURCE_DIR}/genfiles)
file(MAKE_DIRECTORY ${ROOT_GEN_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${ROOT_GEN_DIR})

function(cur_gen_dir out_dir)
  file(RELATIVE_PATH _rel_folder "${PROJECT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")

  set(_tmp_dir ${ROOT_GEN_DIR}/${_rel_folder})
  set(${out_dir} ${_tmp_dir} PARENT_SCOPE)
  file(MAKE_DIRECTORY ${_tmp_dir})
endfunction()

macro(add_include target)
  set_property(TARGET ${target}
               APPEND PROPERTY INCLUDE_DIRECTORIES ${ARGN})
endmacro()

macro(add_compile_flag target)
  set_property(TARGET ${target} APPEND PROPERTY COMPILE_FLAGS ${ARGN})
endmacro()

function(gen_cxx_files list_name extension dirname)
  set(_tmp_l "")
  foreach (_file ${ARGN})
    LIST(APPEND _tmp_l "${dirname}/${_file}.h" "${dirname}/${_file}.${extension}")
  endforeach(_file)
  set(${list_name} ${_tmp_l} PARENT_SCOPE)
endfunction()


function(cxx_link target)
  CMAKE_PARSE_ARGUMENTS(parsed "" "" "DATA" ${ARGN})

  if (parsed_DATA)
    # symlink data files into build directory

    set(run_dir "${CMAKE_BINARY_DIR}/${target}.runfiles")
    foreach (data_file ${parsed_DATA})
      get_filename_component(src_full_path ${data_file} ABSOLUTE)
      if (NOT EXISTS ${src_full_path})
        Message(FATAL_ERROR "Can not find ${src_full_path} when processing ${target}")
      endif()
      set(target_data_full "${run_dir}/${data_file}")
      get_filename_component(target_data_folder ${target_data_full} PATH)
      file(MAKE_DIRECTORY ${target_data_folder})
      execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${src_full_path} ${target_data_full})
    endforeach(data_file)
  endif()

  set(link_depends ${parsed_UNPARSED_ARGUMENTS})
  target_link_libraries(${target} ${link_depends})

endfunction()

function(cxx_proto_lib name)
  cur_gen_dir(gen_dir)

  # create __init__.py in all parent directories that lead to protobuf to support
  # python proto files.
  file(RELATIVE_PATH gen_rel_path ${ROOT_GEN_DIR} ${gen_dir})
  # Message("gen_rel_path ${ROOT_GEN_DIR} ${gen_dir}")  # pumadb/genfiles pumadb/genfiles/gaia/util
  string(REPLACE "/" ";" parent_list ${gen_rel_path})
  set(cur_parent ${ROOT_GEN_DIR})
  foreach (rel_parent ${parent_list})
    set(cur_parent "${cur_parent}/${rel_parent}")
    file(WRITE "${cur_parent}/__init__.py" "")
  endforeach(rel_parent)

  gen_cxx_files(cxx_out_files "cc" ${gen_dir} ${name}.pb)

  GET_FILENAME_COMPONENT(absolute_proto_name ${name}.proto ABSOLUTE)

  CMAKE_PARSE_ARGUMENTS(parsed "PY" "" "DEPENDS;GEN_DEPENDS;PLUGIN_ARGS;EXT_GEN_FILES" ${ARGN})
  set(prefix_command ${PROTOC} ${absolute_proto_name} --proto_path=${PROJECT_SOURCE_DIR} --proto_path=${CMAKE_SOURCE_DIR})

  if (parsed_PY)
    set(py_command COMMAND ${prefix_command} --proto_path=${PROTOBUF_INCLUDE_DIR} --python_out=${ROOT_GEN_DIR}
                   COMMAND touch ${gen_dir}/__init__.py
        )
  endif()
  LIST(APPEND cxx_out_files ${parsed_EXT_GEN_FILES})

  ADD_CUSTOM_COMMAND(
           OUTPUT ${cxx_out_files}
           COMMAND ${PROTOC} ${absolute_proto_name}
                   --proto_path=${PROJECT_SOURCE_DIR} --proto_path=${PROTOBUF_INCLUDE_DIR}
                   --cpp_out=${ROOT_GEN_DIR} ${parsed_PLUGIN_ARGS}
           ${py_command}

           COMMAND CLICOLOR_FORCE=1 ${CMAKE_COMMAND} -E cmake_echo_color --blue --bold "Generating sources '${cxx_out_files}'"
           COMMAND CLICOLOR_FORCE=1 ${CMAKE_COMMAND} -E cmake_echo_color --green --bold "from '${absolute_proto_name}'"
           DEPENDS ${name}.proto protobuf_project ${parsed_DEPENDS} ${parsed_GEN_DEPENDS}
           WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

  set_source_files_properties(${cxx_out_files}
                              PROPERTIES GENERATED TRUE)
  set(lib_name "${name}_proto")
  add_library(${lib_name} ${cxx_out_files})
  target_link_libraries(${lib_name} ${parsed_DEPENDS} TRDP::protobuf)
  add_include(${lib_name} ${PROTOBUF_INCLUDE_DIR})
  add_compile_flag(${lib_name} "-DGOOGLE_PROTOBUF_NO_RTTI -Wno-unused-parameter")
endfunction()

function(flex_lib name)
  GET_FILENAME_COMPONENT(_in ${name}.lex ABSOLUTE)
  cur_gen_dir(gen_dir)
  set(lib_name "${name}_flex")

  set(full_path_cc ${gen_dir}/${name}.cc)
  ADD_CUSTOM_COMMAND(
           OUTPUT ${full_path_cc}
           COMMAND mkdir -p ${gen_dir}
           COMMAND ${CMAKE_COMMAND} -E remove ${gen_dir}/${name}.ih
           COMMAND flex -o ${gen_dir}/${name}.cc ${_in}
           DEPENDS ${_in}
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
           COMMENT "Generating lexer from ${name}.lex" VERBATIM)

  set_source_files_properties(${gen_dir}/${name}.h ${gen_dir}/${name}.cc ${gen_dir}/${name}_base.h
                              PROPERTIES GENERATED TRUE)

  # plang_parser.hh is here because this must be generated after the parser is generated
  add_library(${lib_name} ${gen_dir}/${name}.cc ${gen_dir}/plang_parser.hh)
  add_compile_flag(${lib_name} "-Wno-extra")
  target_link_libraries(${lib_name} TRDP::glog strings)
endfunction()

function(bison_lib name)
  GET_FILENAME_COMPONENT(_in ${name}.y ABSOLUTE)
  cur_gen_dir(gen_dir)
  set(lib_name "${name}_bison")
  add_library(${lib_name} ${gen_dir}/${name}.cc)
  set(full_path_cc ${gen_dir}/${name}.cc ${gen_dir}/${name}.hh)

  ADD_CUSTOM_COMMAND(
           OUTPUT ${full_path_cc}
           COMMAND mkdir -p ${gen_dir}
           COMMAND bison --language=c++ -o ${gen_dir}/${name}.cc ${name}.y
           DEPENDS ${_in}
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
           COMMENT "Generating parser from ${name}.y" VERBATIM)
 set_source_files_properties(${name}.cc ${name}_base.h PROPERTIES GENERATED TRUE)
endfunction()

function(dynasm_lib name)
  GET_FILENAME_COMPONENT(_in ${name}.dasc ABSOLUTE)
  cur_gen_dir(gen_dir)
  set(lib_name "${name}_dasm")
  add_library(${lib_name} ${gen_dir}/${name}.cc)

  set(full_path_cc ${gen_dir}/${name}.cc)
  ADD_CUSTOM_COMMAND(
           OUTPUT ${full_path_cc}
           COMMAND mkdir -p ${gen_dir}

           COMMAND ${DYNASM_COMPILER} ${DYNASM_INCLUDE_DIR}/dynasm/dynasm.lua -o ${full_path_cc} -D X64 ${name}.dasc
           DEPENDS ${_in} DEPENDS dynasm_project
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
           COMMENT "Generating jit compiler from ${name}.dasc" VERBATIM)
 set_source_files_properties(${full_path_cc} PROPERTIES GENERATED TRUE)
 add_include(${lib_name} ${DYNASM_DIR}/include)
endfunction()

SET_PROPERTY(GLOBAL PROPERTY "test_list_property" "")

function(cxx_test name)
  add_executable(${name} ${name}.cc)
  add_dependencies(${name} benchmark_project gtest_project)
  add_compile_flag(${name} "-Wno-sign-compare")
  CMAKE_PARSE_ARGUMENTS(parsed "" "" "LABELS" ${ARGN})

  if (NOT parsed_LABELS)
    set(parsed_LABELS "unit")
  endif()

  add_include(${name} ${GTEST_INCLUDE_DIR} ${BENCHMARK_INCLUDE_DIR})

  cxx_link(${name} gtest_main TRDP::gmock fast_malloc ${parsed_UNPARSED_ARGUMENTS})

  add_test(NAME ${name} COMMAND $<TARGET_FILE:${name}>)
  set_tests_properties(${name} PROPERTIES LABELS "${parsed_LABELS}")
  get_property(cur_list GLOBAL PROPERTY "test_list_property")
  foreach (_label ${parsed_LABELS})
    LIST(APPEND cur_list "${_label}:${name}")
  endforeach(_label)
  SET_PROPERTY(GLOBAL PROPERTY "test_list_property" "${cur_list}")


  # add_custom_command(TARGET ${name} POST_BUILD
  #                    COMMAND ${name} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  #                    COMMENT "Running ${name}" VERBATIM)
endfunction()

function(swig_lib name)
  GET_FILENAME_COMPONENT(_in ${name}.swig ABSOLUTE)
  cur_gen_dir(gen_dir)
  set(lib_name_underscoreless "${name}_swig")
  set(lib_name "_${lib_name_underscoreless}")
  set(full_path_cc ${gen_dir}/${lib_name}.cc)
  ADD_CUSTOM_COMMAND(
    OUTPUT ${full_path_cc}
    COMMAND ${PROJECT_SOURCE_DIR}/scripts/swig_compile.sh ${CMAKE_SOURCE_DIR}
                                                          ${lib_name_underscoreless} ${gen_dir}
                                                          ${_in} ${ARGN}
    DEPENDS ${_in} ${ARGN}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Generating swig wrapper code from ${name}.swig"
    VERBATIM)
  set_source_files_properties(${gen_dir}/${lib_name}.cc PROPERTIES GENERATED TRUE)
  add_library(${lib_name} SHARED ${gen_dir}/${lib_name}.cc)
  add_compile_flag(${lib_name} "-Wno-extra")
  set_target_properties(${lib_name} PROPERTIES PREFIX "")
  target_link_libraries(${lib_name} python2.7)
  add_include(${lib_name} "/usr/include/python2.7/")
  add_custom_target(${lib_name_underscoreless} DEPENDS ${lib_name})
endfunction()

function(jinja_lib name)
  cur_gen_dir(gen_dir)

  set(gen_cc_list "")
  foreach (file ${ARGN})
    get_filename_component(base_name ${file} NAME_WE)
    get_filename_component(fl_ext ${file} EXT)
    string(REGEX MATCH "^(\\.[^.]+)" c_ext ${fl_ext})

    set(out_gen_file "${gen_dir}/${base_name}.jinja${c_ext}")

    LIST(APPEND gen_cc_list ${out_gen_file})
  endforeach(file)

  ADD_CUSTOM_COMMAND(
           OUTPUT ${gen_cc_list}
           COMMAND mkdir -p ${gen_dir}
           COMMAND ${PROJECT_SOURCE_DIR}/scripts/jinja2_compile.py ${ARGN} -o ${gen_dir}
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
           DEPENDS ${ARGN}
           COMMENT "Generating cc from jinja2 files ${ARGN}" VERBATIM)
  set_source_files_properties(${gen_cc_list} PROPERTIES GENERATED TRUE)

  set(target_name "${name}_jinja_target")
  add_custom_target(${target_name} DEPENDS ${gen_cc_list})

  set(lib_name "${name}_jinja_lib")
  add_library(${lib_name} ${gen_cc_list})
  add_dependencies(${lib_name} ${target_name})
endfunction()

