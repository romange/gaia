add_library(double_compressor double_compressor.cc)
cxx_link(double_compressor base math TRDP::lz4 TRDP::blosc TRDP::zstd)

cxx_test(double_compressor_test LABELS CI)
cxx_link(double_compressor_test double_compressor)
