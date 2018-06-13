import logging

import psycopg2

logger = logging.getLogger('root')
TEMP_TABLE_NAME = 'temp_table'


def get_last_upper_bound(job_config):
    query = __generate_status_query(job_config.status_table_name, job_config.source_table_name)
    res = execute_query(job_config.dest_connection_string, query)

    if res[0][0] is None:
        logger.warn("No status found for " + job_config.source_table_name + " will load from scratch")
        return 0

    if len(res) > 1:
        raise Exception("More than one status entry found for " + job_config.source_table_name + " giving up")

    return res[0][0]


def update_upper_bound(job_config, new_upper_bound, is_first_run=False):
    if is_first_run:
        query = __generate_insert_bounds_query(job_config.status_table_name, job_config.source_table_name,
                                               new_upper_bound)
    else:
        query = __generate_update_bounds_query(job_config.status_table_name, job_config.source_table_name,
                                               new_upper_bound)

    execute_query(job_config.dest_connection_string, query, fetch_data=False)


def copy_to_redshift(job_config, target_table, source_bucket, file_name):
    if hasattr(job_config, 's3_copy_override') and job_config.s3_copy_override != "":
        query = __populate_override_copy_query(job_config.s3_copy_override, target_table, source_bucket, file_name,
                                               job_config.redshift_role)

    else:
        query = __generate_copy_query(target_table, source_bucket, file_name,
                                      job_config.redshift_role)

    execute_query(job_config.dest_connection_string, query, fetch_data=False)


def create_temp_table(job_config):
    query = __generate_create_temp_table_query(job_config.dest_table_name)
    execute_query(job_config.dest_connection_string, query, fetch_data=False)

    return TEMP_TABLE_NAME


def insert_from_temp_table(job_config, lower_bound, upper_bound):
    query = __generate_insert_from_temp_table(job_config.dest_table_name, TEMP_TABLE_NAME, job_config.timestamp_col,
                                              lower_bound, upper_bound)
    execute_query(job_config.dest_connection_string, query, fetch_data=False)


def drop_temp_table(job_config):
    query = __generate_drop_temp_table_query()
    execute_query(job_config.dest_connection_string, query, fetch_data=False)


def execute_query(connection_string, query, fetch_data=True):
    connection_string = connection_string
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    logger.info("Will execute " + query)
    cursor.execute(query)

    data = {}
    if fetch_data:
        data = cursor.fetchall()

    conn.commit()
    conn.close()
    cursor.close()

    return data


def __generate_copy_query(dest_table_name, s3_bucket, file_name, iam_role):
    s3_path = "\'s3://%s/%s\'" % (s3_bucket, file_name)
    return "COPY %s FROM %s IAM_ROLE \'%s\' ACCEPTINVCHARS as ' ' IGNOREBLANKLINES FORMAT AS CSV IGNOREHEADER AS 1 " \
           "COMPUPDATE ON TRUNCATECOLUMNS" % (dest_table_name, s3_path, iam_role)


def __generate_status_query(status_table_name, source_table_name):
    return "SELECT MAX(latest_load) FROM %s WHERE source_table_name = \'%s\'" % (status_table_name, source_table_name)


def __generate_update_bounds_query(status_table_name, source_table_name, upper_bound):
    return "UPDATE %s SET latest_load= \'%s\' WHERE source_table_name = \'%s\'" % (
        status_table_name, upper_bound, source_table_name)


def __generate_insert_bounds_query(status_table_name, source_table_name, upper_bound):
    return "INSERT INTO %s VALUES(\'%s\', \'%s\')" % (status_table_name, source_table_name, upper_bound)


def __generate_create_temp_table_query(dest_table_name):
    return "CREATE TABLE %s(like %s)" % (TEMP_TABLE_NAME, dest_table_name)


def __generate_drop_temp_table_query():
    return "DROP TABLE IF EXISTS " + TEMP_TABLE_NAME


def __generate_insert_from_temp_table(dest_table_name, temp_table_name, timestamp_col, lower_bound, upper_bound):
    if lower_bound is 0:
        return "INSERT INTO %s SELECT * FROM %s WHERE %s <= \'%s\'" % (
            dest_table_name, temp_table_name, timestamp_col, upper_bound)

    return "INSERT INTO %s SELECT * FROM %s WHERE %s > \'%s\' and %s <= \'%s\'" % (
        dest_table_name, temp_table_name, timestamp_col, lower_bound, timestamp_col, upper_bound)


def __populate_override_copy_query(s3_override, dest_table_name, s3_bucket, file_name, role):
    s3_path = "\'s3://%s/%s\'" % (s3_bucket, file_name)
    return s3_override \
        .replace("$TABLE_NAME", dest_table_name) \
        .replace("$S3_SOURCE", s3_path) \
        .replace("$ROLE", role)
