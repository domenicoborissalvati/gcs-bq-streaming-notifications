# Streaming data from Cloud Storage into BigQuery using Cloud Functions
This code looks at a complete ingest pipeline all the way from capturing streaming events 
(upload of files to Cloud Storage), to doing basic processing, errorm handling, logging and 
insert stream to bigquery. The example captures events from a bucket (object create) with 
Cloud Function, reads the file and stream the content (CSV) to a table in BigQuery. 

For more details of how to execute the steps of this streaming pipeline, please take a look on 
[Streaming data from Cloud Storage into BigQuery using Cloud Functions](https://cloud.google.com/solutions/streaming-data-from-cloud-storage-into-bigquery-using-cloud-functions) Tutorial.
