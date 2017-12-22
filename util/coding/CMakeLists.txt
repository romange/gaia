add_library(coding double_compressor.cc)
cxx_link(coding base math TRDP::lz4 TRDP::blosc TRDP::zstd)

cxx_test(double_compressor_test LABELS CI)
cxx_link(double_compressor_test coding)
