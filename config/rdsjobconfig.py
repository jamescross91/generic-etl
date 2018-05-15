from config.etljobconfig import ETLJobConfig


class RDSJobConfig(ETLJobConfig):
    def __init__(self, source_connection_string, dest_connection_string, source_table_name, dest_table_name,
                 timestamp_col, s3_bucket_name, status_table_name, redshift_role, panda_transforms, run_before, run_after):

        ETLJobConfig.__init__(self, dest_connection_string, source_table_name, dest_table_name,
                              status_table_name, redshift_role, run_before, run_after)
        self.timestamp_col = timestamp_col
        self.source_connection_string = source_connection_string
        self.s3_bucket_name = s3_bucket_name
        self.panda_transforms = panda_transforms
