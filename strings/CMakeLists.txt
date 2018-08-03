add_library(strings  human_readable.cc
            stringpiece.cc range.cc split.cc strcat.cc stringprintf.cc numbers.cc
            unique_strings.cc)
target_link_libraries(strings base absl_strings)
add_dependencies(strings sparsehash_project)

add_library(strpmr strpmr.cc)
target_link_libraries(strpmr TRDP::pmr strings)


cxx_test(range_test strings LABELS CI)
cxx_test(unique_strings_test strings LABELS CI)
cxx_test(strcat_test strings LABELS CI)
cxx_test(strpmr_test strings LABELS CI)
