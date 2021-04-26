from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    base_insert_query = 'INSERT INTO {} {}'

    @apply_defaults
    def __init__(self,
                 table=None,
                 staging_select_query=None,
                 append_data=False,
                 redshift_conn_id='redshift',
                 *args,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.staging_select_query = staging_select_query
        self.append_data = append_data

        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_connection = PostgresHook(self.redshift_conn_id)

        if not self.append_data:
            self.log.info(f'Wiping data from {self.table} table')
            redshift_connection.run(f'DELETE FROM {self.table}')

        self.log.info(f'Loading fact table {self.table} from staging')
        insert_query = self.base_insert_query.format(self.table,
                                                     self.staging_select_query)
        redshift_connection.run(insert_query)
