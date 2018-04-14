import datetime
import logging

import boto3

from job.redshift import get_last_upper_bound, create_temp_table, copy_to_redshift, insert_from_temp_table, \
    drop_temp_table, update_upper_bound, execute_query

logger = logging.getLogger('root')


def run_s3_job(job_config):
    last_upper_bound = get_last_upper_bound(job_config)
    new_upper_bound = datetime.datetime.now()

    files_to_process = __get_files_in_source_bucket(job_config)
    files_to_process = sum(1 for _ in files_to_process)
    logger.info("Found " + str(files_to_process) + " file(s) to process")

    if files_to_process > 0:
        for s3_object in files_to_process:
            logger.info("Processing " + s3_object.key)
            __process_one_file(job_config, s3_object.key, last_upper_bound, new_upper_bound)

        __execute_sql_statements(job_config)
        update_upper_bound(job_config, new_upper_bound, is_first_run=last_upper_bound == 0)


def __execute_sql_statements(job_config):
    for statement in job_config.sql_statements:
        execute_query(job_config.dest_connection_string, statement)


def __process_one_file(job_config, filename, lower_bound, upper_bound):
    drop_temp_table(job_config)
    temp_table_name = create_temp_table(job_config)
    copy_to_redshift(job_config, temp_table_name, job_config.source_s3_bucket, filename)
    insert_from_temp_table(job_config, lower_bound, upper_bound)
    drop_temp_table(job_config)
    __move_from_source_to_destination(job_config, filename)


def __move_from_source_to_destination(job_config, filename):
    logger.info("Moving " + filename + " from " + job_config.source_s3_bucket + " to " + job_config.dest_s3_bucket)
    s3 = boto3.resource('s3')
    source_path = job_config.source_s3_bucket + "/" + filename
    s3.Object(job_config.dest_s3_bucket, filename).copy_from(CopySource=source_path)
    s3.Object(job_config.source_s3_bucket, filename).delete()


def __get_files_in_source_bucket(job_config):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(job_config.source_s3_bucket)
    return bucket.objects.all()
