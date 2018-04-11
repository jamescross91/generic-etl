class JobConfig:
    def __init__(self, source_connection_string, dest_connection_string, source_table_name, dest_table_name,
                 timestamp_col, s3_bucket_name, status_table_name, redshift_role):
        self.source_connection_string = source_connection_string
        self.dest_connection_string = dest_connection_string
        self.source_table_name = source_table_name
        self.dest_table_name = dest_table_name
        self.timestamp_col = timestamp_col
        self.s3_bucket_name = s3_bucket_name
        self.status_table_name = status_table_name
        self.redshift_role = redshift_role
