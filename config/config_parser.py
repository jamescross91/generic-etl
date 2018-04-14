import json

from config.rdsjobconfig import RDSJobConfig
from config.s3jobconfig import S3JobConfig
from config.sqljobconfig import SQLJobConfig


def from_json(file_path):
    data = json.load(open(file_path))

    source_connection_string = data['source_connection_string']
    dest_connection_string = data['dest_connection_string']
    status_table_name = data['status_table_name']
    redshift_role = data['redshift_role']

    rds_jobs = []
    s3_jobs = []
    destination_sql_jobs = []

    for rds_job in data['rds_jobs']:
        rds_jobs.append(RDSJobConfig(source_connection_string, dest_connection_string, rds_job['source_table_name'],
                                     rds_job['dest_table_name'], rds_job['timestamp_col'], rds_job['s3_bucket_name'],
                                     status_table_name, redshift_role, rds_job['sql_statements']))

    for s3_job in data['s3_jobs']:
        s3_jobs.append(S3JobConfig(s3_job['source_s3_bucket'], s3_job['dest_s3_bucket'], dest_connection_string,
                                   s3_job['dest_table_name'], s3_job['timestamp_col'], status_table_name,
                                   redshift_role, s3_job['sql_statements']))

    for destination_sql_job in data['destination_sql_jobs']:
        destination_sql_jobs.append(SQLJobConfig(dest_connection_string, destination_sql_job['sql_statements']))

    return {
        "rds_jobs": rds_jobs,
        "s3_jobs": s3_jobs,
        "destination_sql_jobs": destination_sql_jobs
    }
