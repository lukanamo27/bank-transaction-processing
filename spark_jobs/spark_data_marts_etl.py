from docutils.nodes import option
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum as _sum, avg, month
from utils.bigquery_utils import create_dataset_if_not_exists, \
    load_spark_df_to_bq
from utils.bigquery_utils import create_table_if_not_exists
from utils.logger import logger
import sys
from google.cloud import bigquery

logger.info('Starting pyspark job for data marts creation')

spark = SparkSession.builder.appName('Bank Data Marts ETL').getOrCreate()

VALID_TABLE_NAME = sys.argv[1]
DATASET_NAME = sys.argv[2]
DATAPROC_TEMP_BUCKET = sys.argv[3]

interval_duration = '6 HOUR'
query = f"""
    SELECT * FROM `{DATASET_NAME}.{VALID_TABLE_NAME}`
    WHERE upload_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {interval_duration})
"""

valid_data_df = spark.read \
    .format('bigquery') \
    .option('query', query) \
    .option("dataset", DATASET_NAME) \
    .option("table", VALID_TABLE_NAME) \
    .load()

logger.info(f'Successfully read valid data from {VALID_TABLE_NAME}')

if valid_data_df:
    logger.info(f'Valid data: {valid_data_df.show(n=5)}')
    datamarts_dataset_name = 'datamarts_dataset'
    create_dataset_if_not_exists(datamarts_dataset_name)

    # datamart 1
    transactions_per_customer_df = valid_data_df.groupby('customer_id') \
        .agg(
        count('transaction_id').alias('num_transactions'),
        _sum('amount').alias('total_amount'),
        avg('amount').alias('average_transaction_amount')
    )

    tpc_table_name = 'transactions_per_customer'
    tpc_schema = [
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("num_transactions", "INTEGER", ),
        bigquery.SchemaField("total_amount", "FLOAT"),
        bigquery.SchemaField("average_transaction_amount", "FLOAT")
    ]

    create_table_if_not_exists(
        datamarts_dataset_name, tpc_table_name, tpc_schema
    )

    load_spark_df_to_bq(
        transactions_per_customer_df, datamarts_dataset_name, tpc_table_name,
        DATAPROC_TEMP_BUCKET
    )

    logger.info(f'Datamart {tpc_table_name} created successfully')

    # datamart 2
    transactions_per_merchant_df = valid_data_df.groupby('merchant_id') \
        .agg(
        count('transaction_id').alias('num_transactions'),
        _sum('amount').alias('total_sales')
    )

    tpm_table_name = 'transaction_per_merchant'

    tpm_schema = [
        bigquery.SchemaField("merchant_id", "STRING"),
        bigquery.SchemaField("num_transactions", "INTEGER", ),
        bigquery.SchemaField("total_sales", "FLOAT")
    ]

    create_table_if_not_exists(
        datamarts_dataset_name, tpm_table_name, tpm_schema
    )

    load_spark_df_to_bq(
        transactions_per_merchant_df, datamarts_dataset_name, tpm_table_name,
        DATAPROC_TEMP_BUCKET
    )

    logger.info(f'Datamart {tpm_table_name} created successfully')

    # datamart 3
    monthly_transactions_df = valid_data_df.withColumn(
        'transaction_month', month('timestamp')
    ).groupby('transaction_month').agg(
        count('transaction_id').alias('num_transactions'),
        _sum('amount').alias('total_amount')
    )

    mt_table_name = 'monthly_transactions'
    mt_schema = [
        bigquery.SchemaField("transaction_month", "INTEGER"),
        bigquery.SchemaField("num_transactions", "INTEGER"),
        bigquery.SchemaField("total_amount", "FLOAT")
    ]

    create_table_if_not_exists(datamarts_dataset_name, mt_table_name,
                               mt_schema)

    load_spark_df_to_bq(
        monthly_transactions_df, datamarts_dataset_name, mt_table_name,
        DATAPROC_TEMP_BUCKET
    )

    logger.info(f'Datamart {mt_table_name} created successfully')
else:
    logger.info(f'No data has been received in past {interval_duration}s')
