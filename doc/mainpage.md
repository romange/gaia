Welcome to GAIA-MR
============

Multi-threaded, efficient mapreduce framework for data processing.

## Parallel cloud GREP
Suppose we want a simple tool that can read and grep text files, maybe compressed, or maybe stored
on cloud storage.Here is how [we can do it](https://github.com/romange/gaia/blob/master/examples/mrgrep.cc).

```cpp
class Grepper {
  RE2 re_;
 public:
  Grepper(string reg_exp) : re_(reg_exp) {}

  void Do(string val, mr3::DoContext<GsodRecord>* context) {
    if (RE2::PartialMatch(val, re_)) {
      cout << context->raw()->input_file_name() << ": " << val << endl;
    }
  }
};

int main(int argc, char** argv) {
  PipelineMain pm(&argc, &argv);

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
  return 0;
}
```
