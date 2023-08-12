from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    select_sql_format = 'SELECT COUNT(*) FROM {} WHERE {} IS NULL'

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                # Example:
                # conn_id = your-connection-name
                redshift_id = 'redshift',
                tables = {},
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_id = redshift_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Begin DataQualityOperator')
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_id)

        for table, pkey in self.tables.items():
            self.log.info(f'Executing data quality checks for {table} table')
            query_result = postgres_hook.get_records(DataQualityOperator.select_sql_format.format(table, pkey))
            
            self.log.info(f'The query result of {table}: {query_result}')
            if len(query_result) > 0 and query_result[0][0] > 0:
                self.log.error(f'Data quality check failed for {table} table')
                raise ValueError(f'Data quality check failed for {table} table')
        
            self.log.info(f'Data quality check passed for {table} table')
        
        self.log.info('End DataQualityOperator for all tables')