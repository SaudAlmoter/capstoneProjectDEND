from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
#     Operator that loads data from staging table to the fact table.

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,

                 conn_id,
                 table,
                 create_query,
                 insert_query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.table = table
        self.create_query = create_query
        self.insert_query = insert_query

    def execute(self, context):
        self.log.info('LoadFactOperator is starting the execution')
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info('Creating {} table if it does not exist...'.format(self.table))
        self.hook.run(self.create_query)

        self.log.info("Removing data from {}".format(self.table))
        self.hook.run("DELETE FROM {}".format(self.table))

        self.log.info('Inserting data from staging table...')
        self.hook.run(self.insert_query)
        self.log.info("Insert execution completed...")
        
