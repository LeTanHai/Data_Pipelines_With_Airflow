from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                # Example:
                # conn_id = your-connection-name
                redshift_id = 'redshift',
                sql_file = 'create_table.sql',
                first_flag = False,
                *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_id = redshift_id
        self.sql_file = sql_file
        self.first_flag = first_flag

    def execute(self, context):

        if self.first_flag:
            self.log.info(f'Executing query create table')
            postgres_hook = PostgresHook(postgres_conn_id=self.redshift_id)

            with open(self.sql_file, 'r') as file:
                query = file.read()

            postgres_hook.run(query)

        self.log.info('End CreateTableOperator table')
