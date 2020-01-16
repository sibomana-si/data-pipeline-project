from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list=[],
                 check_stmt="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
        self.check_stmt = check_stmt
        self.expected_result = expected_result

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)                 
        self.log.info("Null value Check.")
        for table in self.table_list:
            records = redshift.get_records(self.check_stmt.format(table[0], table[1]))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table[0]} returned no results")
            num_records = records[0][0]
            if num_records != self.expected_result:
                raise ValueError(f"Null value check failed. {table[0]} contains {records[0][0]} rows with null values in primary key column '{table[1]}'.")
            self.log.info(f"Null value check on table {table[0]} passed.")
                
            