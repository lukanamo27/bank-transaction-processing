from airflow.providers.google.cloud.operators.dataproc import \
    DataprocSubmitJobOperator
from google.cloud.dataproc import Job
from utils.logger import logger


def submit_spark_job(
        dataproc_cluster_name: str,
        dataproc_region: str,
        path_to_pyspark_job: str,
        pyspark_job_args: list[str] | None = None,
        python_file_uris: list[str] | None = None
):
    try:
        logger.info("Submitting Spark job.")
        spark_job = Job(
            placement={'cluster_name': dataproc_cluster_name},
            pyspark_job={
                'main_python_file_uri': path_to_pyspark_job,
                'args': pyspark_job_args,
                'python_file_uris': python_file_uris
            }
        )

        spark_job_operator = DataprocSubmitJobOperator(
            task_id='submit_spark_job',
            job=spark_job,
            region=dataproc_region
        )

        logger.info("Dataproc job submitted successfully.")
        return spark_job_operator
    except Exception as e:
        logger.error(f"Error submitting Spark job: {e}")
        raise
