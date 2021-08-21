from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    create_table_sql = """
                            CREATE TABLE IF NOT EXISTS songplays (
                                playid varchar(32) NOT NULL,
                                start_time timestamp NOT NULL,
                                userid int4 NOT NULL,
                                "level" varchar(256),
                                songid varchar(256),
                                artistid varchar(256),
                                sessionid int4,
                                location varchar(256),
                                user_agent varchar(256),
                                CONSTRAINT songplays_pkey PRIMARY KEY (playid)
                            );
                       """
    insert_table_snippet = """
                                INSERT INTO songplays(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
                           """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Creating fact table songplays')
        redshift.run(LoadFactOperator.create_table_sql)
        
        self.log.info('Loading fact table songplays')
        redshift.run(LoadFactOperator.insert_table_snippet + "\n" + SqlQueries.songplay_table_insert)
