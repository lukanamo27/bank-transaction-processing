from google.cloud import storage
from pyspark.sql import SparkSession, DataFrame
from utils.logger import logger
from pyspark.sql.types import StructType

storage_client = storage.Client()


def extract_data_from_gcs_to_spark_df(bucket_name: str, file_name: str,
                                      spark_session: SparkSession,
                                      schema: StructType) -> DataFrame:
    path = f'gs://{bucket_name}/{file_name}'
    try:
        logger.info(
            f"Extracting data from GCS bucket: {bucket_name}, file: {file_name}")
        df = spark_session.read.json(path, schema=schema)
        logger.info(
            f"Data extraction from GCS successful for file: {file_name}")
        return df
    except Exception as e:
        logger.error(f"Error extracting data from GCS: {e}")
        raise
