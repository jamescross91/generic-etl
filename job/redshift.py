import logging

import psycopg2

logger = logging.getLogger('root')


def get_last_upper_bound(job_config):
    connection_string = job_config.dest_connection_string
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    query = __generate_status_query(job_config.status_table_name, job_config.source_table_name)
    logger.info("Will execute " + query)

    cursor.execute(query)
    res = cursor.fetchall()

    if res[0][0] is None:
        logger.warn("No status found for " + job_config.source_table_name + " will load from scratch")
        return 0

    if len(res) > 1:
        raise Exception("More than one status entry found for " + job_config.source_table_name + " giving up")

    return res[0][0]


def __generate_status_query(status_table_name, source_table_name):
    return "select max(latest_load) from %s where source_table_name = \'%s\'" % (status_table_name, source_table_name)
