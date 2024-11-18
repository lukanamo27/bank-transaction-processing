from google.cloud import storage
from pyspark.sql import SparkSession, DataFrame
from utils.logger import logger
from pyspark.sql.types import StructType

storage_client = storage.Client()


def extract_data_from_gcs_to_spark_df(
        bucket_name: str, dir_name: str,
        spark_session: SparkSession,
        schema: StructType
) -> DataFrame:
    logger.info(f'Collecting file paths from gs://{bucket_name}/{dir_name}')
    blobs = storage_client.list_blobs(bucket_name, prefix=dir_name)
    file_paths = [
        f'gs://{bucket_name}/{blob.name}' for blob in blobs \
        if not blob.name.endswith('/')
    ]

    if not file_paths:
        logger.warning(f"No files found in dir: {dir_name}")
        return spark_session.createDataFrame([], schema)

    logger.info(f"Found {len(file_paths)} files in dir: {dir_name}")
    logger.info(f'File paths: {file_paths}')

    try:
        df = spark_session.read.json(file_paths, schema=schema)
        logger.info(
            f'Data extraction from GCS dir: {dir_name} successful'
        )
        return df

    except Exception as e:
        logger.error(f"Error extracting data from GCS: {e}")
        raise
