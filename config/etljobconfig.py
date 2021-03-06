class ETLJobConfig:
    def __init__(self, dest_connection_string, source_table_name, dest_table_name, status_table_name,
                 redshift_role, run_before, run_after):
        self.dest_connection_string = dest_connection_string
        self.status_table_name = status_table_name
        self.redshift_role = redshift_role
        self.dest_table_name = dest_table_name
        self.source_table_name = source_table_name
        self.run_before = run_before
        self.run_after = run_after
