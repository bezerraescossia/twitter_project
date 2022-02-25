from datetime import datetime
from email.mime import application
from os.path import join

from numpy import partition
from airflow.models import DAG
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from pathlib import Path


ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(6),
}
BASE_FOLDER = join(
    str(Path('~/Github').expanduser()),
    'studies/twitter-project/datalake/{stage}/{partition}'
)
PARTITION_FOLDER = 'extract_date={{ ds }}'
TIMESTAMP_FORMAT='%Y-%m-%dT%H:%M:%S.00Z'

with DAG(dag_id="twitter_dag", default_args=ARGS, schedule_interval='0 9 * * *', max_active_runs=1) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path=join(BASE_FOLDER.format(stage='bronze', partition=PARTITION_FOLDER),
            "AluraOnline_{{ ds_nodash }}.json"
            ),
        start_time=(
            "{{"
            f"execution_date.strftime('{ TIMESTAMP_FORMAT }')"
            "}}"
            ),
        end_time=(
            "{{"
            f"next_execution_date.strftime('{ TIMESTAMP_FORMAT }')"
            "}}"
            )
    )

    twitter_transform = SparkSubmitOperator(
        task_id='transform_twitter_aluraonline',
        application=join(
            str(Path(__file__).parents[2]),
            'spark/transformation.py'
            ),
        name='twitter_transformation',
        application_args=[
            '--src', BASE_FOLDER.format(stage='bronze', partition=PARTITION_FOLDER),
            '--dest', BASE_FOLDER.format(stage='silver', partition=''),
            '--process-date', '{{ ds }}']
    )

    twitter_operator >> twitter_transform