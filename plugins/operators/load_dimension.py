from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    delete_sql_format = "DELETE FROM {}"
    insert_sql_format = "INSERT INTO {} {}"

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                # Example:
                # conn_id = your-connection-name
                redshift_id = 'redshift',
                table_name = '',
                sql_statement = '',
                delete_data = False,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_id = redshift_id
        self.table_name = table_name
        self.sql_statement = sql_statement
        self.delete_data = delete_data

    def execute(self, context):
        self.log.info(f'Begin LoadDimensionOperator for {self.table_name} table')
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_id)

        if self.delete_data == True:
            self.log.info(f'Executing delete data from {self.table_name} table before loading dimension data')
            postgres_hook.run(LoadDimensionOperator.delete_sql_format.format(self.table_name))

        self.log.info(f'Executing insert data into {self.table_name} table')
        postgres_hook.run(LoadDimensionOperator.insert_sql_format.format(self.table_name, self.sql_statement))
        self.log.info('End LoadDimensionOperator for {self.table_name} table')
