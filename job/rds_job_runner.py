import datetime
import logging

import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
import boto3
import uuid
from job.redshift import get_last_upper_bound, copy_to_redshift, update_upper_bound, execute_query

logger = logging.getLogger('root')


def run_rds_job(job_config):
    logger.info("Running RDS ETL for " + job_config.source_table_name)
    last_upper_bound = get_last_upper_bound(job_config)
    new_upper_bound = datetime.datetime.now()

    df = __read_from_source(job_config, last_upper_bound, new_upper_bound)
    records_to_process = len(df.index)
    logger.info("Received " + str(records_to_process) + " records to store in S3")

    if records_to_process > 0:
        file_name = __to_s3(job_config, df, last_upper_bound)
        copy_to_redshift(job_config, job_config.dest_table_name, job_config.s3_bucket_name, file_name)
        __execute_sql_statements(job_config)
        update_upper_bound(job_config, new_upper_bound, is_first_run=last_upper_bound == 0)

    logger.info("################################### COMPLETE ###################################")


def __to_s3(job_config, df, new_upper_bound):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, encoding='utf-8', index=False)
    s3_resource = boto3.resource('s3')
    file_name = __generate_file_name(job_config.source_table_name, new_upper_bound)
    logger.info("Using bucket " + job_config.s3_bucket_name + " to write file " + file_name)
    s3_resource.Object(job_config.s3_bucket_name, file_name).put(Body=csv_buffer.getvalue())

    return file_name


def __execute_sql_statements(job_config):
    for statement in job_config.sql_statements:
        execute_query(job_config.dest_connection_string, statement)


def __read_from_source(job_config, last_upper_bound, new_upper_bound):
    engine = create_engine(job_config.source_connection_string, echo=False)
    sql = __generate_query(job_config.source_table_name, job_config.timestamp_col, last_upper_bound, new_upper_bound)
    logger.info("Will execute " + sql)
    df = pd.read_sql(sql, engine)
    return df


def __generate_query(source_table_name, timestamp_col, timestamp_lower_bound, timestamp_upper_bound):
    if timestamp_lower_bound is 0:
        return "Select * from %s where %s <= \'%s\'" % (source_table_name, timestamp_col, str(timestamp_upper_bound))

    return "Select * from %s where %s > \'%s\' and %s <= \'%s\'" % (
        source_table_name, timestamp_col, timestamp_lower_bound, timestamp_col, str(timestamp_upper_bound))


def __generate_file_name(source_table_name, timestamp_upper_bound):
    return source_table_name + "_" + str(timestamp_upper_bound) + "_" + str(uuid.uuid4()) + ".csv"
