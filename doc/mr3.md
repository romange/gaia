## GSOD weather - tutorial

[gsod_group.cc](../examples/gsod_group.cc) file demonstrates a map/reshard/group pattern
that often is needed when processing large datasets. The framework provides just partitioning (resharding),
while joining (or grouping) is done in user code. This way, unlike in `Beam` or similar frameworks,
the developer has more control on how to reduce unnecesary I/O.

### Preliminaries
The first example we will cover is processing of
[GSOD weather dataset](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=samples&t=gsod&page=table) from [Google Bigquery Public Datasets](https://cloud.google.com/bigquery/public-data/).

In order to read its data we need to export this table to GCS first.
I've prepared publicly accessible sampled dataset at `gs://kushkush/gsod/` that you can copy
to you local disk or access it directly from mr3. For latter, I suggest you to run the pipeline from cloud instance
in order to allow maximum performance. You will also need gcloud sdk installed with `~/.config/gcloud/`
directory if you access GCS files directly from mr3.

### Reading inputs
At first, we instruct the pipeline to read text files and skip the first line in each file.
The input files could be compressed or uncompressed - that's transparent to user and is
auto-detected upon read. Both gzip and zstd compressions are supported.
Text files are treated as bag of lines and the pipeline will process those lines independently
from each other. In general any mr3 stream is represented as unordered, possibly sharded
list of records using C++ handle `PTable<MyRecordType>`. In this case it's just unsharded `PTable<string>`.
In addition, the input files could be read from the local disk or from gcs storage. For example,
```console
./gsod_group gs://kushkush/gsod/gsod-shards-0*
```

or
```console
./gsod_group '/tmp/gsod/gsod-shards-0*'
```
are both valid invocations. The framework will expand GCS prefix or a bash glob accordingly. Note, that the framework currently supports GCS globs with '*' at the end, or '**' for the recursive glob.

Then we instruct our pipeline to run our mapper to parse each line into a meaningful record.
In this case our files are in CSV format and we decided that
we extract the columns we need into `GsodRecord`. In particular we keep only a `year` and `station` columns.

To make our own C++ class `GsodRecord` serializable within `mr3` we must specialize
`template <> class mr3::RecordTraits<GsodRecord>` with 2 methods `Serialize` and `Parse`.
They will be used later when our mapper outputs the extracted records using `DoContext<GsodRecord>`.

Our mapper is expected to have a hook method `void Do(InputType val, mr3::DoContext<OutputType>* context)`.
In our case `InputType` is `std::string` since we process string table and `OutputType=GsodRecord`.
Please note that the framework determines based on the signature of `GsodMapper::Do` the type of the
result table `PTable<GsodRecord>`.

The mapper can output as many records as it wishes upon each input record it processes or
not output at all. This allows to filter, expand or do other transformations on the data.

In addition, a mapper can have `void OnShardFinish(DoContext<OutputType>* context);` hook that
will be called when the mapper has finished processing a batch of records scheduled by the framework.
Anyway, in our case `GsodMapper` just outputs a single `GsodRecord` record per input line.

~~~~~~~~~~cpp
StringTable ss = pipeline->ReadText("gsod", inputs).set_skip_header(1);
PTable<GsodRecord> records = ss.Map<GsodMapper>("MapToGsod");
~~~~~~~~~~

### Resharding
In order to cope with large amounts of data that can not be hold in RAM,
our framework allows to repartition or as we call it 're-shard' the data before applying
next the operator.

```cpp
records.Write("gsod_map", pb::WireFormat::TXT)
      .WithModNSharding(10, [](const GsodRecord& r) { return r.year; })
      .AndCompress(pb::Output::GZIP);
```

This line instructs the framework to reshard the mapped table of GsodRecords by year into 10 shards.
The final shards will also be compressed. Resharding is crucial to bring records of particular
property together so that we could load them into RAM. Since we used ModN sharding it most likely that
each file shard will contain multiple years of data but every unique year will be hold by exactly
one file shard. The developer is expected to choose shards count in such way that the input data divided
by number of shards will be less than `total RAM available` / `number of cores on the machine`.
The number of shards is bounded from above by file limit of the system (
  it's usually less than 10K though it's customizable). Producing only few shards
is also not very good, because it might affect the level of parallelism when running the next operator.

### GroupBy
Finally we apply the operator that processed each shard assuming that entities of the same year
are located together. Please note that unlike with other frameworks the operator does not get any guarantees
on the order of the entities inside shard and must handle them by its own. Low guarantees put more
reponsibility on a developer but provide more performant framework with less I/O.

`Join` operator can read multiple sharded inputs, in case we want to join multiple sources of data.
In order to bind each input with its own handler function we have `records.BindWith(&GsodJoiner::Group)`
call that tell the framework that in this case we want that `records` table will be handled by `GsodJoiner::Group`
handler. In our case we have only one input that we want to process by counting all the records per year.

As we said earlier, each shard might span multiple years of data so we use absl hash table to count
number of records per year. After a shard is processed, the framework calls
`void GsodJoiner::OnShardFinish` where our operator outputs the counts per year it processed.

```cpp
StringTable joined =
      pipeline->Join<GsodJoiner>("group_by", {records.BindWith(&GsodJoiner::Group)});
```

### Running the pipeline
All the commands above only configure the framework with user-provided operators and bind them
with the appropriate inputs. The entry point that triggers the run is the call `pipeline->Run(runner);`.
The framework comes with already implemented `Runner` that provides fiber friendly,
multi-threaded single machine processing. This `LocalRunner` requires a destination directory to store
its intermediate and final outputs. Once the run is completed the process finishes.

```
LocalRunner* runner = pm.StartLocalRunner(FLAGS_dest_dir);
pipeline->Run(runner);
```
