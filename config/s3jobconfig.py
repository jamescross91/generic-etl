from config.etljobconfig import ETLJobConfig


class S3JobConfig(ETLJobConfig):
    def __init__(self, source_s3_bucket, dest_s3_bucket, dest_connection_string, dest_table_name, timestamp_col,
                 status_table_name, redshift_role, run_before, run_after):
        ETLJobConfig.__init__(self, dest_connection_string, dest_table_name, dest_table_name, timestamp_col,
                              status_table_name, redshift_role, run_before, run_after)

        self.source_s3_bucket = source_s3_bucket
        self.dest_s3_bucket = dest_s3_bucket
