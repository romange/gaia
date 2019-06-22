## GSOD weather - tutorial

### Preliminaries
The first example we will cover is processing of
[GSOD weather dataset](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=samples&t=gsod&page=table) from [Google Bigquery Public Datasets](https://cloud.google.com/bigquery/public-data/).

In order to read its data we need to export this table to GCS first.
I've prepared publicly accessible sampled dataset at `gs://kushkush/gsod/` that you can copy
to you local disk or access it directly from mr3. For latter, I suggest you to run the pipeline from cloud instance
in order to allow maximum performance. You will also need gcloud sdk installed with `~/.config/gcloud/`
directory if you access GCS directly from mr3.

[gsod_group.cc](examples/gsod_group.cc) Demonstrates a map/reshard/group pattern that often is needed
when processing large datasets.

At first we instruct the pipeline to read text files and skip the first line in each file.
The input files could be compressed or uncompressed - that's transparent to user and is
auto-detected upon read. Both gzip and zstd compressions are supported.
Text files are treated as bag of lines and the pipeline will process those lines independently
from each other. In general any mr3 stream is represented as unordered, possibly partitioned (sharded)
list of records using C++ handle `PTable<MyRecordType>`. In this case it's just `PTable<string>`.

Then we instruct our pipeline to run our mapper that knows how to parse each line into a meaningful record.
In this case our files are in CSV format and we decided that
we extract the columns we need into `GsodRecord` and in particular we keep only a year and station columns.

To make our own C++ class `GsodRecord` serializable within `mr3` we must specialize
`template <> class mr3::RecordTraits<GsodRecord>` with 2 methods `Serialize` and `Parse`.
They will be used later when our mapper outputs the extracted records using `DoContext<GsodRecord>`.

Our mapper is expected to have a hook method `void Do(InputType val, mr3::DoContext<OutputType>* context)`.
In our case `InputType` is `std::string` since we process string table and `OutputType=GsodRecord`.
Please note that the framework determines based on the signature of `GsodMapper::Do` the type of the
result table `PTable<GsodRecord>`.

The mapper can output as many records as we wish upon each input record it processes or
not output at all. This allows to filter, expand or do other types of transformations.
In addition, a mapper can have `void OnShardFinish(DoContext<OutputType>* context);` hook that
will be called when the mapper has finished processing a batch of records scheduled by the framework.
In this case `GsodMapper` just outputs a single `GsodRecord` record per input line.

~~~~~~~~~~cpp
StringTable ss = pipeline->ReadText("gsod", inputs).set_skip_header(1);
PTable<GsodRecord> records = ss.Map<GsodMapper>("MapToGsod");
~~~~~~~~~~

