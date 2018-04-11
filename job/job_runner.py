import datetime

import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
import boto3
import uuid

import log
from job.redshift import get_last_upper_bound

logger = log.configure_logging('root')


def run_job(job_config):
    last_upper_bound = get_last_upper_bound(job_config)
    new_upper_bound = datetime.datetime.now().time()
    __to_s3(job_config, last_upper_bound, new_upper_bound)

    print()


def __to_s3(job_config, last_upper_bound, new_upper_bound):
    engine = create_engine(job_config.connection_string, echo=False)
    sql = __generate_query(job_config.source_table_name, job_config.timestamp_col, last_upper_bound, new_upper_bound)
    logger.info("Will execute " + sql)
    df = pd.read_sql(sql, engine)
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, encoding='utf-8')
    s3_resource = boto3.resource('s3')
    file_name = __generate_file_name(job_config.source_table_name, new_upper_bound)
    logger.info("Using bucket " + job_config.s3_bucket_name + " to write file " + file_name)
    s3_resource.Object(job_config.s3_bucket_name, file_name).put(Body=csv_buffer.getvalue())


def __generate_query(source_table_name, timestamp_col, timestamp_lower_bound, timestamp_upper_bound):
    return "Select * from %s where %s > \'%s\' and %s <= \'%s\'" % (
        source_table_name, timestamp_col, timestamp_lower_bound, timestamp_col, timestamp_upper_bound)


def __generate_file_name(source_table_name, timestamp_upper_bound):
    return source_table_name + "_" + timestamp_upper_bound + "_" + str(uuid.uuid4()) + ".csv"
