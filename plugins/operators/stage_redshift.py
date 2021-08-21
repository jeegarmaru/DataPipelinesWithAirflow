from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-west-2'
        format as JSON '{}'
    """
    
    create_table_sqls = ["""
                            CREATE TABLE IF NOT EXISTS staging_events (
                                artist varchar(256),
                                auth varchar(256),
                                firstname varchar(256),
                                gender varchar(256),
                                iteminsession int4,
                                lastname varchar(256),
                                length numeric(18,0),
                                "level" varchar(256),
                                location varchar(256),
                                "method" varchar(256),
                                page varchar(256),
                                registration numeric(18,0),
                                sessionid int4,
                                song varchar(256),
                                status int4,
                                ts int8,
                                useragent varchar(256),
                                userid int4
                            );
                         """,
                         """
                            CREATE TABLE IF NOT EXISTS staging_songs(
                                num_songs int4,
                                artist_id varchar(256),
                                artist_name varchar(256),
                                artist_latitude numeric(18,0),
                                artist_longitude numeric(18,0),
                                artist_location varchar(256),
                                song_id varchar(256),
                                title varchar(256),
                                duration numeric(18,0),
                                "year" int4
                            );
                         """
                        ]

    @apply_defaults
    def __init__(self,
                 aws_credentials='aws_credentials',
                 redshift_conn_id='redshift',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_format='auto',
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        aws_creds = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Creating table {self.table}")
        for sql in StageToRedshiftOperator.create_table_sqls:
            if self.table in sql:
                redshift.run(sql)
        
        self.log.info(f'Copying from S3 to Redshift for table {self.table}')
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        copy_sql = StageToRedshiftOperator.copy_sql_template.format(
            self.table,
            s3_path,
            aws_creds.access_key,
            aws_creds.secret_key,
            self.json_format
        )
        redshift.run(copy_sql)

