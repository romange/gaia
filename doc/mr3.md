## GSOD weather - tutorial

### Preliminaries
The first example we will cover is processing of
[GSOD weather dataset](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=samples&t=gsod&page=table) from [Google Bigquery Public Datasets](https://cloud.google.com/bigquery/public-data/).
To read its data we need to export this table to GCS first.
I've prepared publicly accessible sample of this export at `gs://kushkush/gsod/` that you can copy
to you local disk or access it directly.
For latter case, to allow maximum performance I suggest you to run the pipeline from cloud instance.
You will also need gcloud sdk installed with `~/.config/gcloud/` directory.

