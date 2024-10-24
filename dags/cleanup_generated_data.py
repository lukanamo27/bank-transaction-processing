from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.google.cloud.operators.gcs import \
    GCSDeleteObjectsOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils.state import DagRunState

from utils.logger import logger

BUCKET_NAME = Variable.get('BUCKET_NAME')
FILES_TO_DELETE = Variable.get('FILES_TO_DELETE', deserialize_json=True)

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

    # wait_for_transactions_etl = ExternalTaskSensor(
    #     task_id='wait_for_transactions_etl',
    #     external_dag_id='bank_transactions_etl',
    #     external_task_id=None,
    #     allowed_states=[DagRunState.SUCCESS],
    #     timeout=600,
    #     failed_states=[DagRunState.FAILED],
    #     mode='poke'
    # )

    delete_files = GCSDeleteObjectsOperator(
        task_id='delete_generated_data',
        bucket_name=BUCKET_NAME,
        objects=FILES_TO_DELETE
    )

    # wait_for_transactions_etl >> delete_files
    delete_files


cleanup_generated_data = cleanup_generated_data()
