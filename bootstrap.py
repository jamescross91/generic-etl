import log
from config.config_parser import from_json
from job.job_runner import run_job

logger = log.configure_logging('root')


def run():
    job_configs = from_json("/Users/James/Developer/fendix/generic-etl/configs/etl_config.json")

    for job_config in job_configs:
        run_job(job_config)


run()
