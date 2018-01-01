add_library(coding double_compressor.cc block_compressor.cc)
cxx_link(coding base math TRDP::lz4 TRDP::blosc TRDP::zstd)

add_library(set_encoder_lib set_encoder.cc sequence_array.cc)
cxx_link(set_encoder_lib strings coding)

cxx_test(double_compressor_test coding LABELS CI)
cxx_test(block_compressor_test coding LABELS CI)

cxx_test(set_encoder_test LABELS CI)
cxx_link(set_encoder_test set_encoder_lib)

