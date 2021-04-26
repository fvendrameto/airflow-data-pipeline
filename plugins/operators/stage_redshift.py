from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    base_copy_query = ("""
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        REGION '{}';
        """)

    @apply_defaults
    def __init__(self,
                 table=None,
                 s3_bucket=None,
                 s3_key=None,
                 json_format='auto',
                 region='us-west-2',
                 aws_credentials_id='aws_credentials',
                 redshift_conn_id='redshift',
                 *args,
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        self.region = region

        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift_connection = PostgresHook(self.redshift_conn_id)

        self.log.info('Wiping data from staging table')
        redshift_connection.run(f'DELETE FROM {self.table}')

        self.log.info('Copying data from S3 to staging table')

        formatted_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, formatted_key)

        copy_query = self.base_copy_query.format(self.table, s3_path,
                                                 credentials.access_key,
                                                 credentials.secret_key,
                                                 self.json_format, self.region)

        redshift_connection.run(copy_query)
