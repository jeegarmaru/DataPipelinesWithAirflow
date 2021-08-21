from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    create_table_sqls = {
        'artists': """
                        CREATE TABLE IF NOT EXISTS artists (
                            artistid varchar(256) NOT NULL,
                            name varchar(256),
                            location varchar(256),
                            lattitude numeric(18,0),
                            longitude numeric(18,0)
                        );
        """,
        'songs': """
                    CREATE TABLE IF NOT EXISTS songs (
                        songid varchar(256) NOT NULL,
                        title varchar(256),
                        artistid varchar(256),
                        "year" int4,
                        duration numeric(18,0),
                        CONSTRAINT songs_pkey PRIMARY KEY (songid)
                    );
        """,
        'time': """
                    CREATE TABLE IF NOT EXISTS "time" (
                        start_time timestamp NOT NULL,
                        "hour" int4,
                        "day" int4,
                        week int4,
                        "month" varchar(256),
                        "year" int4,
                        weekday varchar(256),
                        CONSTRAINT time_pkey PRIMARY KEY (start_time)
                    );
        """,
        'users': """
                    CREATE TABLE IF NOT EXISTS users2 (
                        userid int4 NOT NULL,
                        first_name varchar(256),
                        last_name varchar(256),
                        gender varchar(256),
                        "level" varchar(256),
                        CONSTRAINT users_pkey PRIMARY KEY (userid)
                    );
        """
    }
    
    insert_table_snippets = {
        'users': 'INSERT INTO users2 (userid, first_name, last_name, gender, level)',
        'artists': 'INSERT INTO artists (artistid, name, location, lattitude, longitude)',
        'songs': 'INSERT INTO songs (songid, title, artistid, year, duration)',
        'time': 'INSERT INTO time (start_time, hour, day, week, month, year, weekday)'
    }
    
    load_table_sqls = {
        'users': SqlQueries.user_table_insert,
        'artists': SqlQueries.artist_table_insert,
        'time': SqlQueries.time_table_insert,
        'songs': SqlQueries.song_table_insert
    }
    
    truncate_table_sqls = {
        'users': 'TRUNCATE TABLE users2',
        'artists': 'TRUNCATE TABLE artists',
        'time': 'TRUNCATE TABLE time',
        'songs': 'TRUNCATE TABLE songs'
    }

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='redshift',
                 table='',
                 truncate_first=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_first = truncate_first

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Creating dimension table {self.table}')
        redshift.run(LoadDimensionOperator.create_table_sqls[self.table])
        
        if self.truncate_first:
            self.log.info(f'Truncating dimension table {self.table}')
            redshift.run(LoadDimensionOperator.truncate_table_sqls[self.table])

        self.log.info(f'Loading data into dimension table {self.table}')
        redshift.run(LoadDimensionOperator.insert_table_snippets[self.table] + "\n" + LoadDimensionOperator.load_table_sqls[self.table])

