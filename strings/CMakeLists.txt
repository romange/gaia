add_library(strings ascii_ctype.cc charset.cc join.cc human_readable.cc escaping.cc
            strcat.cc stringpiece.cc range.cc
            stringprintf.cc split.cc strip.cc util.cc numbers.cc strtoint.cc
            unique_strings.cc)
target_link_libraries(strings base)
add_dependencies(strings sparsehash_project)

add_library(strpmr strpmr.cc)
target_link_libraries(strpmr TRDP::pmr strings)


cxx_test(range_test strings LABELS CI)
cxx_test(unique_strings_test strings LABELS CI)
cxx_test(urlencode_test strings LABELS CI)
cxx_test(strcat_test strings LABELS CI)
cxx_test(strpmr_test strings LABELS CI)
