from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358145'
    template_fields = ("s3_key",)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """

    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 log_json_file="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = s3_hook.get_credentials()
    
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Delete data from Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("S3 to Redshift copy")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.log_json_file != "":
            self.log_json_file = "s3://{}/{}".format(
                self.s3_bucket, self.log_json_file)
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.log_json_file
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                "auto"
            )
        redshift.run(formatted_sql)
        self.log.info(
            f"Success: S3 to Redshift data copied in table {self.table}")
