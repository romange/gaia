#include <re2/re2.h>

#include "base/logging.h"
#include "file/file_util.h"

#include "mr/local_runner.h"
#include "mr/mr_main.h"

using namespace mr3;
using namespace util;
using namespace std;
using re2::RE2;

DEFINE_string(e, "",
              "Regular expression with perl-like syntax.\n"
              "See https://github.com/google/re2/wiki/Syntax for details.");

class Grepper {
 public:
  Grepper(string reg_exp) : re_(reg_exp) {
    CHECK(re_.ok()) << "Error parsing 'e': " << re_.error_code() << " offending part "
                    << re_.error_arg();
  }

  void Do(string val, DoContext<string>* context) {
    if (RE2::PartialMatch(val, re_)) {
      auto* raw = context->raw();
      cout << raw->input_file_name() << ":" << raw->input_pos() << " " << val << endl;
    }
  }

 private:
  RE2 re_;  // Even though RE2 is thread-safe we allocate separate instances to reduce contention.
};

int main(int argc, char** argv) {
  PipelineMain pm(&argc, &argv);
  if (argc < 2) {
    return 0;
  }

  vector<string> inputs;
  for (int i = 1; i < argc; ++i) {
    inputs.push_back(argv[i]);
  }
  CHECK(!FLAGS_e.empty());

  Pipeline* pipeline = pm.pipeline();
  StringTable st = pipeline->ReadText("read_input", inputs);
  StringTable no_output = st.Map<Grepper>("grep", FLAGS_e);
  no_output.Write("null", pb::WireFormat::TXT);

  LocalRunner* runner = pm.StartLocalRunner("/tmp/");
  pipeline->Run(runner);

  LOG(INFO) << "After pipeline run";

  return 0;
}
