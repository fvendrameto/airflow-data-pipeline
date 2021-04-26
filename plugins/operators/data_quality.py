from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    base_count_rows_query = 'SELECT COUNT(*) FROM {}'
    base_check_null_query = ('SELECT COUNT({column}) FROM {table} '
                             'WHERE {column} IS NULL')

    @apply_defaults
    def __init__(self,
                 all_checks=None,
                 redshift_conn_id='redshift',
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.all_checks = all_checks

        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_connection = PostgresHook(self.redshift_conn_id)

        for check in self.all_checks:
            query = check['query']
            expected_result = check['expected_result']

            records = redshift_connection.get_records(query)[0][0]
            
            has_null_msg = (f'Data quality check failed. Expected query '
                            f'"{query}" to return {expected_result}, '
                            f'got {records} instead')

            assert records == expected_result, has_null_msg
            
            success_msg = (f'Data quality check "{query}" passed.')
            self.log.info(success_msg)
