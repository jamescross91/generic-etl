from job.redshift import execute_query


def run_sql_job(job_config):
    for sql_statement in job_config.sql_statements:
        execute_query(job_config.connection_string, sql_statement, fetch_data=False)