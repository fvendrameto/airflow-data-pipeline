from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    base_insert_query = 'INSERT INTO {} {}'

    @apply_defaults
    def __init__(self,
                 table=None,
                 staging_select_query=None,
                 redshift_conn_id='redshift',
                 *args,
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.staging_select_query = staging_select_query

        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_connection = PostgresHook(self.redshift_conn_id)
        self.log.info(f'Loading dimension table {self.table} from staging')

        insert_query = self.base_insert_query.format(self.table,
                                                     self.staging_select_query)
        redshift_connection.run(insert_query)
