# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
This Cloud function, triggered whene a new .csv file is finalized inside the
source Storage bucket, inserts the records in the destination BigQuery table. 
- source/destination are declared at first
- prints some useful logs for debugging
"""

import json
import pandas
import os
from google.api_core import retry
from google.cloud import bigquery


PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'notifications'
BQ_TABLE = 'push_notifications_v2'
BQ = bigquery.Client()


def streaming_notifications2(data, context):
    """
    Entry point
    """
    event = context.event_id
    print('Event ID: {}'.format(event))
    bucket_name = data['bucket']
    print('Bucket: {}'.format(bucket_name))
    file_name = data['name']
    print('File: {}'.format(file_name))
    created_at = data['timeCreated']
    print('Created at: {}'.format(created_at))
    records = _read_csv(bucket_name, file_name)
    if len(records)>0:
        try:
            _insert_into_bigquery(records)
        except Exception:
            print("failed to insert this data into BigQuery")
    else:
        print("file has no records to insert into BigQuery")


def _read_csv(bucket_name, file_name):
    """
    Reads .csv file in a Google Storage bucket
    Returns a dictionary with records
    """
    df = pandas.read_csv('gs://'+bucket_name+'/'+file_name, encoding='utf-8')
    records_dict = df.to_dict('records')
    return records_dict


def _insert_into_bigquery(dict):
    """
    Reads the .csv file (trigger) in the bucket and
    Appends the records to the google bigquery table
    """
    table_ref = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    table = BQ.get_table(table_ref)
    errors = BQ.insert_rows(table,
                            rows=dict,
                            retry=retry.Retry(deadline=30))
    if errors != []:
        raise BigQueryError(errors)


class BigQueryError(Exception):
    """
    Exception raised whenever a BigQuery error happened
    """

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)