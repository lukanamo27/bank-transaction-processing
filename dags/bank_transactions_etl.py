from datetime import timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from tasks.submit_spark_job import submit_spark_job
from utils.logger import logger
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'start_date': days_ago(1),
    'retries': 0
}


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
        dataproc_cluster_name=Variable.get('DATAPROC_CLUSTER_NAME'),
        dataproc_region=Variable.get('DATAPROC_REGION'),
        path_to_pyspark_job=Variable.get('PATH_TO_PYSPARK_ETL_JOB'),
        pyspark_job_args=[
            Variable.get('BUCKET_NAME'), Variable.get('CUSTOMER_DATA_PATH'),
            Variable.get('TRANSACTION_DATA_PATH'),
            Variable.get('DATASET_NAME'), Variable.get('VALID_TABLE_NAME'),
            Variable.get('INVALID_TABLE_NAME'),
            Variable.get('DATAPROC_TEMP_BUCKET')
        ],
        python_file_uris=[Variable.get('CUSTOM_PACKAGE_URI')]
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


dag = bank_transactions_etl()
