from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class S3ToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ["s3_key"]
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as csv '{}' compupdate off region 'us-west-2';
    """

    @apply_defaults
    def __init__(self,
                 table: str,
                 redshift_conn_id: str,
                 aws_credentials_id: str,
                 s3_bucket: str,
                 s3_key: str,
                 csv_option: str,
                 *args, **kwargs):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.csv_option = csv_option

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info('S3 bucket path: ', s3_path)
        self.log.info('Copying data to: {}'.format(self.table))
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.csv_option,
        )
        redshift.run(formatted_sql)