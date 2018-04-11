import logging

import psycopg2

logger = logging.getLogger('root')


def get_last_upper_bound(job_config):
    query = __generate_status_query(job_config.status_table_name, job_config.source_table_name)
    res = __execute_query(job_config.dest_connection_string, query)

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

    __execute_query(job_config.dest_connection_string, query, fetch_data=False)


def copy_to_redshift(job_config, file_name):
    query = __generate_copy_query(job_config.dest_table_name, job_config.s3_bucket_name, file_name,
                                  job_config.redshift_role)

    __execute_query(job_config.dest_connection_string, query, fetch_data=False)


def __execute_query(connection_string, query, fetch_data=True):
    connection_string = connection_string
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(query)
    logger.info("Will execute " + query)

    data = {}
    if fetch_data:
        data = cursor.fetchall()

    conn.commit()
    conn.close()
    cursor.close()

    return data


def __generate_copy_query(dest_table_name, s3_bucket, file_name, iam_role):
    s3_path = "\'s3://%s/%s\'" % (s3_bucket, file_name)
    return "COPY %s FROM %s IAM_ROLE \'%s\' FORMAT AS CSV IGNOREHEADER AS 1" % (dest_table_name, s3_path, iam_role)


def __generate_status_query(status_table_name, source_table_name):
    return "SELECT MAX(latest_load) FROM %s WHERE source_table_name = \'%s\'" % (status_table_name, source_table_name)


def __generate_update_bounds_query(status_table_name, source_table_name, upper_bound):
    return "UPDATE %s SET latest_load= \'%s\' WHERE source_table_name = \'%s\'" % (status_table_name, upper_bound, source_table_name)


def __generate_insert_bounds_query(status_table_name, source_table_name, upper_bound):
    return "INSERT INTO %s VALUES(\'%s\', \'%s\')" % (status_table_name, source_table_name, upper_bound)
