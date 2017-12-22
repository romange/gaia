add_library(math mathlimits.cc mathutil.cc float2decimal.cc exactfloat/exactfloat.cc)
cxx_link(math base crypto)

cxx_test(float2decimal_test math TRDP::dconv LABELS CI)
