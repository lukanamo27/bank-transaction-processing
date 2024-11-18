from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.dates import days_ago
from tasks.submit_spark_job import submit_spark_job
from utils.logger import logger

default_args = {
    'start_date': days_ago(1),
    'retries': 0
}


@dag(
    dag_id='bank_data_marts_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Submit spark job to create data marts from the bank transaction data'
)
def bank_data_marts_etl():
    logger.info('Starting the bank_data_marts_etl DAG.')

    etl_task = submit_spark_job(
        dataproc_region=Variable.get('DATAPROC_REGION'),
        dataproc_cluster_name=Variable.get('DATAPROC_CLUSTER_NAME'),
        path_to_pyspark_job=Variable.get('PATH_TO_PYSPARK_DATA_MARTS_JOB'),
        pyspark_job_args=[
            Variable.get('VALID_TABLE_NAME'), Variable.get('DATASET_NAME'),
            Variable.get('DATAPROC_TEMP_BUCKET')
        ],
        python_file_uris=[Variable.get('CUSTOM_PACKAGE_URI')]
    )

    # wait_for_transactions_etl >> etl_task
    etl_task

    logger.info('ETL task for data marts scheduled.')


bank_data_marts_etl_dag = bank_data_marts_etl()
