from datetime import timedelta
from airflow.decorators import dag
from airflow.models import Variable
from airflow.utils.dates import days_ago
from tasks.submit_spark_job import submit_spark_job
from utils.logger import logger
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# dataproc args
DATAPROC_REGION = Variable.get('DATAPROC_REGION')
DATAPROC_CLUSTER_NAME = Variable.get('DATAPROC_CLUSTER_NAME')
PATH_TO_PYSPARK_ETL_JOB = Variable.get('PATH_TO_PYSPARK_ETL_JOB')

# pyspark job args
BUCKET_NAME = Variable.get('BUCKET_NAME')
CUSTOMER_FILE_NAME = Variable.get('CUSTOMER_FILE_NAME')
TRANSACTION_FILE_NAME = Variable.get('TRANSACTION_FILE_NAME')
DATASET_NAME = Variable.get('DATASET_NAME')
VALID_TABLE_NAME = Variable.get('VALID_TABLE_NAME')
INVALID_TABLE_NAME = Variable.get('INVALID_TABLE_NAME')
DATAPROC_TEMP_BUCKET = Variable.get('DATAPROC_TEMP_BUCKET')

# python_file_uris
CUSTOM_PACKAGE_URI = Variable.get('CUSTOM_PACKAGE_URI')

default_args = {
    'start_date': days_ago(1),
    'retries': 0
}


# noinspection PyStatementEffect
@dag(
    dag_id='bank_transactions_etl',
    default_args=default_args,
    schedule_interval=timedelta(hours=6),
    catchup=False,
    description='Submit Spark job to perform ETL'
)
def bank_transactions_etl():
    logger.info("Starting the bank_transactions_etl DAG.")

    etl_task = submit_spark_job(
        dataproc_cluster_name=DATAPROC_CLUSTER_NAME,
        dataproc_region=DATAPROC_REGION,
        path_to_pyspark_job=PATH_TO_PYSPARK_ETL_JOB,
        pyspark_job_args=[
            BUCKET_NAME, CUSTOMER_FILE_NAME, TRANSACTION_FILE_NAME,
            DATASET_NAME, VALID_TABLE_NAME,INVALID_TABLE_NAME,
            DATAPROC_TEMP_BUCKET
        ],
        python_file_uris=[CUSTOM_PACKAGE_URI]
    )

    trigger_datamarts_dag = TriggerDagRunOperator(
        task_id='trigger_datamarts_dag',
        trigger_dag_id='bank_data_marts_etl',
        wait_for_completion=False
    )

    trigger_cleanup_dag = TriggerDagRunOperator(
        task_id='trigger_cleanup_dag',
        trigger_dag_id='cleanup_generated_data',
        wait_for_completion=False
    )

    etl_task >> [trigger_datamarts_dag, trigger_cleanup_dag]

    logger.info("ETL task scheduled.")

bank_transactions_etl_dag = bank_transactions_etl()
