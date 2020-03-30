Introduction to Gaia MR
=======================

Overview
--------

A C++ Gaia MR program will usually be composed of three types of objects (note that all of these have the same C++ type, PTable): readers, mappers, and joiners.

A reader is responsible for reading input files of a specific type, it is usually initialized via a glob URL and a format, possibly including some metadata.

A mapper runs a mapping function on each input it gets, outputting one or more values per input. Mappers also shard the data according to a sharding function, allowing a joiner to apply a function to all inputs that it gets under the same shard.

A joiner runs on every shard, reading the output from all mappers that outputted to it and later outputs some values based on what it read.

The above descriptions are pure idealized descriptions. In real life, both mapper and joiner are stateful, both support outputting values both during shard finish and for every input that they get, and both can shard their outputs. This means that mapper and joiner can likely be easily generalized into one type of object.

Furthremore, both mapper and joiner support writing to counters and frequency maps. Counters are key-value maps that are used to collect statistics per object. They are outputted to logs and possibly to online monitoring. Frequency maps act as a global shared state, shared between all threads and *MR phases*. This means that one can update the frequency map in one phase and read the updates in another.

Example (code simplified from `word_count.cc`)
----------------------------------------------

Create a pipeline object.

```
PipelineMain pm(&argc, &argv);
Pipeline* pipeline = pm.pipeline();
```

Read the wanted inputs.

```
std::vector<std::string> inputs;
// some code here should fill inputs.
PTable<std::string> read = pipeline->ReadText("inp1", inputs);
```

For every input, convert it into word-count pairs, shard outputs by word. `WordSplitter` is a class that implements the map operation. Note how `Map` is called from the `read` object, connecting the map operation with the inputs read earlier.

```
PTable<WordCount> intermediate_table = read.Map<WordSplitter>("word_splitter", db);
intermediate_table.Write("word_interim", pb::WireFormat::TXT)
    .WithModNSharding(FLAGS_num_shards,
                      [](const WordCount& wc) { return base::Fingerprint(wc.word); })
    .AndCompress(pb::Output::ZSTD, FLAGS_compress_level);
```

At this point, it is guaranteed that all instances of a word-count pair for the same word will appear in the same shard. This means that if we sum counts all over a shard, for every shard, we will get only one word-count pair per word, thus getting the correct counts. `WordCount` is a class that implements the join operation. Note how `Join` accepts a list of bindings to mappers, this can be used to connect an arbitrary number of mappers to a single joiner.

```
  PTable<WordCount> word_counts = pipeline->Join<WordGroupBy>(
      "group_by", {intermediate_table.BindWith(&WordGroupBy::OnWordCount)});
  word_counts.Write("wordcounts", pb::WireFormat::TXT)
      .AndCompress(pb::Output::ZSTD, FLAGS_compress_level);
```

Run the pipeline.

```
  LocalRunner* runner = pm.StartLocalRunner(FLAGS_dest_dir);
  pipeline->Run(runner);
```

Let's take a look at how the mapper and the joiner are implemented. A mapper has a `Do` function that is called for every input (the first parameter). It also accepts a `DoContext` object which allows it to output data, and update counters (in this example, the `matched` counter) and frequency maps. A mapper can also have an `OnShardFinish` that is called once a mapper is done reading a shard. In the full version of `word_count.cc`, one can that when `FLAGS_use_combine` is on, `WordSplitter` uses internal state and a `OnShardFinish` to run some accumulation in memory before it outputs to disk, thus doing some of the joiner's job.

```
void WordSplitter::Do(string line, DoContext<WordCount>* cntx) {
  MatchData md{this, line.c_str(), cntx};

  StringPiece line_re2(line), word;

  while (RE2::FindAndConsume(&line_re2, *re_, &word)) {
    OnMatch(0, word, cntx);
  }
}

void WordSplitter::OnMatch(unsigned int id, StringPiece word, DoContext<WordCount>* cntx) {
  cntx->Write(WordCount{string(word), 1});
  cntx->raw()->Inc("matched");
}
```

A joiner's per-input function doesn't have a fixed name. Instead, it is bound using the `BindWith` call (see above). In this case, it is the function `OnWordCount` that is called per input. It stores the words in an accumulating on-memory table. The table is outputted by `OnShardFinish` which is called, just like in the mapper, once reading the entire shard is done. Note that by default, unlike a mapper, a joiner writes to a new shard with the same id as its input shard, although this can be changed.

```
class WordGroupBy {
 public:
  void OnWordCount(WordCount wc, DoContext<WordCount>* context) {
    word_table_.AddWord(wc.word, wc.cnt);
  }

  void OnShardFinish(DoContext<WordCount>* cntx) { word_table_.Flush(cntx); }

 private:
  WordCountTable word_table_;
};
```

What happens when one runs a pipeline
-------------------------------------

When one calls the `Pipeline::Join` or `PTable<T>::Map` methods, a `PTable<T>` object is created, which is a wrapper around a `detail::TableImplT`. The `detail::TableImplT` is given a factory function which gets a `RawContext` (see below) and generates `HandlerWrapperBase` objects. These objects represent the interface between executors (the classes which run join/map logic, see below) and the user-provided code. The three main interfaces provided by `HandlerWrapperBase` are: `SetGroupingShard` which calls the user provided `OnShardStart`, `Get(i)` which returns the i-th input handler for a join or the single input handler for a map, and `OnShardFinish` which calls the user-provided function of the same name.

When one calls the `PTable<T>::Write` method, it adds the mapper/joiner into `Pipeline::tables_`. Mapper/joiners are translated into a protobuf based representation, discarding template magic. When one calls `Pipeline::Run`, it begins iterating on `tables_`, creating and running an executor object (`JoinerExecutor` or `MapperExecutor`) for each entry, and merging together several per-thread counters and frequency.

The executor object is responsible for the actual execution of the joiner/mapper. Before talking about executors, it is important to discuss the idea of an `IoContextPool`. In essence, an `IoContextPool` is an object that creates a thread pool where each thread is pinned to a single CPU. On these threads there is also an event loop, allowing several fibers (cooperative sub-threads) to run. The event loop in each thread waits for lambdas to be sent to it to run. Executors use this object in order to parallelize the work-load in an efficient, context-switchless way.

A `MapperExecutor` creates 2 fibers per thread, the first (`IOReadFiber`) is responsible for reading the data (either input data or output data from a previous mapper/joiner) and the second (`MapFiber`) is responsible to repeatedly call the `Do` function on the mapper. The two fibers communicate via a queue object (`record_q`).

A `JoinerExecutor` creates a single fiber `ProcessInputQ` per thread which constantly alternates between reading from a file and calling the callback. Note that this is less efficient than the 2 fibers of `MapperExecutor` and in the future may be replaced (or even better, unified with `MapperExecutor`). The `JoinerExecutor` also makes sure to read all of the files from mapeprs of the same shard before calling `OnShardFinish()`.

Some forms of IO storage (for example, Google Storage) work better when you read simultaneously instead of serially. Because of this, `MapperExecutor` allows one to duplicate the number of fibers it creates via `FLAGS_map_io_read_factor`. This flag's value is by default 2, which means that the previous paragraph was not accurate, `MapperExecutor` actually opens 4 fibers per thread, two `IOReadFiber`s and two `MapFiber`s (note that there's still a 1:1 messaging relationship between an `IOReadFiber` and a `MapFiber`).

Note that the idealized model of a thread per CPU doesn't actually work on Linux when reading files from local disk. Linux doesn't support async IO for any filesystem that is not XFS. Because of this, there is a separate pool of threads that only run IO calls and send their results to the caller. This problem doesn't exist when working with Google Storage, since Asio is capable of handling asynchronous network IO.

Although both types of executors handle the logic of the mapping/joining process, they try to avoid doing the I/O operation directly. Instead, I/O operations are implemented by a `Runner` object which represents the interactions of the MR infrastructure with the disk/net.

When an executor begins it calls the runner's `OperatorStart` function, which prepares the machinery for reading/writing files. Afterwards each worker thread in the I/O pool calls the runner's `CreateContext` to get a `RawContext` object. In order to read its inputs, the executor calls the `Runner`'s `ProcessInputFile` function, this function accepts an input path and a callback, it calls the callback repeatedly on inputs coming from the path. Finally, when an executor finishes, it calls the runner's OperatorEnd function, which closes the handles of open files as well as outputs a list of which files were written to (it's impossible to calculate this before running, due to custom sharding).

In the previous paragraph, `RawContext` objects were mentioned. `RawContext` in an essence, can be thought of as a part of `Runner` that is given to worker threads and allows them to write outputs from their MR execution into files (in fact, the `DoContext` that user-written functions accept is a simple wrapper around `RawContext`). Note, however, that `RawContext`, unlike `Runner`, is not purely an abstract interface, it also contains logic to handle freq counters instead of only supporting I/O operations.
