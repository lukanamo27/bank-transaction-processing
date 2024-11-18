from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.google.cloud.operators.gcs import \
    GCSDeleteObjectsOperator
from airflow.utils.dates import days_ago
from utils.logger import logger

default_args = {
    'start_date': days_ago(1),
    'retries': 0
}


@dag(
    dag_id='cleanup_generated_data',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG to delete generated data from storage after ETL completion'
)
def cleanup_generated_data():
    logger.info('Starting the cleanup_generated_data DAG.')

    delete_files = GCSDeleteObjectsOperator(
        task_id='delete_generated_data',
        bucket_name=Variable.get('BUCKET_NAME'),
        prefix=Variable.get('DIR_TO_DELETE')
    )

    # wait_for_transactions_etl >> delete_files
    delete_files


cleanup_generated_data = cleanup_generated_data()
