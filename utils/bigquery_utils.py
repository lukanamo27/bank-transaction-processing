from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from utils.logger import logger
from pyspark.sql import DataFrame

bq_client = bigquery.Client()

# returns?!
def create_dataset_if_not_exists(dataset_name: str):
    dataset_ref = bigquery.DatasetReference(bq_client.project, dataset_name)

    try:
        bq_client.get_dataset(dataset_ref)
        logger.info(f'Dataset {dataset_name} already exists')
        return "done"
    except NotFound:
        logger.info(f'Dataset {dataset_name} not found. Creating Dataset.')
        dataset = bigquery.Dataset(dataset_ref)
        bq_client.create_dataset(dataset)
        logger.info(f'Dataset {dataset_name} created successfully')
        # return "oh nooo"


def create_table_if_not_exists(
        dataset_name: str, table_name: str, schema: list[bigquery.SchemaField],
        partition_parameters: dict | None = None
):
    table_ref = bigquery.TableReference(
        bigquery.DatasetReference(bq_client.project, dataset_name),
        table_name
    )

    try:
        bq_client.get_table(table_ref)
        logger.info(f'Table {table_name} already exists')
    except NotFound:
        logger.info(f'Table {table_name} not found. Creating table.')
        table = bigquery.Table(table_ref, schema=schema)
        if partition_parameters:
            partition_field = partition_parameters['partition_field']
            partition_type = partition_parameters['partition_type']
            if partition_type == 'DAY':
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )
            elif partition_type == 'HOUR':
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.HOUR,
                    field=partition_field
                )
            else:
                logger.info(
                    'Unknown partitioning type. Creating non-partitioned table')
        bq_client.create_table(table)
        logger.info(f'Table {table_name} created successfully')


def load_spark_df_to_bq(data: DataFrame, dataset_name: str, table_name: str,
                        temp_gcs_bucket: str):
    try:
        logger.info(
            f"Loading data into bq. Dataset: {dataset_name}, Table: {table_name}")
        data.write \
            .format('bigquery') \
            .option('table', f'{dataset_name}.{table_name}') \
            .option('temporaryGcsBucket', temp_gcs_bucket) \
            .mode("append") \
            .save()
        logger.info(
            f"Data successfully loaded into bq table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to load data into bq. Error: {e}")
        raise
