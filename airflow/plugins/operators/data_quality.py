from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
     
    # Operator that runs data checks against the inserted data.
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,

                 conn_id,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.tables = tables
        self.hook = PostgresHook(postgres_conn_id = self.conn_id)

    def execute(self, context):
        self.log.info('DataQualityOperator is starting the execution')
        for table in self.tables:
            data = self.hook.get_records(f" SELECT count(*) FROM {table}")
            if len(data) < 1 or len(data[0]) < 1:
                raise ValueError(f"Data quality check failed. {table}  no results returned")
            if data[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} empty table")
            self.log.info(f"Data quality on table {table} check passed with {data[0][0]} records")            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            