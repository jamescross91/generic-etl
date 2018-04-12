from config.jobconfig import JobConfig


class S3JobConfig(JobConfig):
    def __init__(self, source_s3_bucket, dest_s3_bucket, dest_connection_string, dest_table_name, timestamp_col,
                 status_table_name, redshift_role):
        JobConfig.__init__(self, dest_connection_string, dest_table_name, dest_table_name, timestamp_col,
                           status_table_name,
                           redshift_role)

        self.source_s3_bucket = source_s3_bucket
        self.dest_s3_bucket = dest_s3_bucket
