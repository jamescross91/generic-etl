import log
from config.config_parser import from_json
from job.rds_job_runner import run_rds_job
from job.s3_job_runner import run_s3_job

logger = log.configure_logging('root')


def run():
    job_configs = from_json("configs/etl_config.json")

    # for job_config in job_configs["rds_jobs"]:
    #     run_rds_job(job_config)

    for job_config in job_configs["s3_jobs"]:
        run_s3_job(job_config)


run()
