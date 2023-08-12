from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    delete_sql_format = "DELETE FROM {}"
    copy_sql_format = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT as json '{}'
    """

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                # Example:
                # redshift_conn_id=your-connection-name
                redshift_id = 'redshift',
                table_name = '',
                s3_bucket = '',
                s3_key = '',
                aws_credentials_id = 'aws_credentials',
                region = '',
                log_json_path = 'auto',
                delete_data = False, 
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_id = redshift_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.region = region
        self.log_json_path = log_json_path
        self.delete_data = delete_data

    def execute(self, context):
        self.log.info(f'Begin StageToRedshiftOperator of {self.table_name} table')

        postres_hook = PostgresHook(postgres_conn_id=self.redshift_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()

        if self.delete_data == True:
            self.log.info(f"Executing query delete data from {self.table_name} table before loading data into staging table")
            postres_hook.run(StageToRedshiftOperator.delete_sql_format.format(self.table_name))

        s3_object_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        copy_sql_statement = StageToRedshiftOperator.copy_sql_format.format(
            self.table_name,
            s3_object_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.region,
            self.log_json_path
        )

        self.log.info(f"Executing query copy data from {s3_object_path} bucket into {self.table_name} table")
        postres_hook.run(copy_sql_statement)

        self.log.info(f'End StageToRedshiftOperator of {self.table_name} table')
