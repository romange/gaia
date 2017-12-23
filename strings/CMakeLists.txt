add_library(strings unique_strings.cc stringpiece.cc)
target_link_libraries(strings base absl::strings)
add_dependencies(strings TRDP::sparsehash)

#add_library(strpmr strpmr.cc)
# target_link_libraries(strpmr TRDP::pmr strings)

# 
#cxx_test(range_test strings LABELS CI)
cxx_test(unique_strings_test strings)
# cxx_test(strpmr_test strings)
