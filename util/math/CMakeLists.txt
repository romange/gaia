add_library(math mathlimits.cc mathutil.cc float2decimal.cc)
cxx_link(math base strings crypto)

cxx_test(float2decimal_test math strings TRDP::dconv LABELS CI)
