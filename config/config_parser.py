import json

from config.jobconfig import JobConfig


def from_json(file_path):
    data = json.load(open(file_path))

    source_connection_string = data['source_connection_string']
    dest_connection_string = data['dest_connection_string']
    s3_bucket_name = data['s3_bucket_name']
    status_table_name = data['status_table_name']

    job_configs = []

    for job in data[u'jobs']:
        job_configs.append(JobConfig(source_connection_string, dest_connection_string, job['source_table_name'], job['dest_table_name'], job['timestamp_col'], s3_bucket_name, status_table_name))

    return job_configs
