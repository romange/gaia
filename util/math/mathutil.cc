// Copyright 2008 Google Inc. All Rights Reserved.

#include "util/math/mathutil.h"
#include <vector>
using std::vector;

#include "base/integral_types.h"
#include "base/logging.h"

double MathUtil::Harmonic(int64 const n, double *const e) {
  CHECK_GT(n, 0);

  //   Hn ~ ln(n) + 0.5772156649 +
  //        + 1/(2n) - 1/(12n^2) + 1/(120n^4) - error,
  //   with 0 < error < 1/(256*n^4).

  double const
    d = static_cast<double>(n),
    d2 = d * d,
    d4 = d2 * d2;

  return (log(d) + 0.5772156649)  // ln + Gamma constant
    + 1 / (2 * d) - 1 / (12 * d2) + 1 / (120 * d4)
    - (*e = 1 / (256 * d4));
}
