add_library(coding double_compressor.cc)
cxx_link(coding base TRDP::lz4 TRDP::blosc TRDP::zstd)