from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, current_timestamp
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, \
    IntegerType, BooleanType, FloatType, TimestampType
from google.cloud import bigquery
import sys
from utils.bigquery_utils import create_dataset_if_not_exists, \
    create_table_if_not_exists, load_spark_df_to_bq
from utils.gcs_utils import extract_data_from_gcs_to_spark_df
from utils.logger import logger

logger.info('Starting spark job')
spark = SparkSession.builder.appName('Bank Transactions ETL').getOrCreate()

BUCKET_NAME = sys.argv[1]
CUSTOMER_DATA_PATH = sys.argv[2]
TRANSACTION_DATA_PATH = sys.argv[3]
DATASET_NAME = sys.argv[4]
VALID_TABLE_NAME = sys.argv[5]
INVALID_TABLE_NAME = sys.argv[6]
DATAPROC_TEMP_BUCKET = sys.argv[7]

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("fullname", StringType(), False),
    StructField("job", StringType(), True),
    StructField("marital", StringType(), True),
    StructField("balance", FloatType(), True),
    StructField("housing", StringType(), True),
    StructField("loan", BooleanType(), True),
    StructField("contact", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("geography", StringType(), True),
    StructField("number_of_products", IntegerType(), True),
    StructField("tenure", IntegerType(), True),
    StructField("loan_number", IntegerType(), True)
])

TRANSACTION_SCHEMA = StructType([
    StructField("currency", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("amount", FloatType(), False),
    StructField("total_amount", FloatType(), False),
    StructField("status", StringType(), True),
    StructField("customer_id", StringType(), False),
    StructField("merchant_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("deposit_method", StringType(), True),
    StructField("installment", BooleanType(), True),
    StructField("installment_amount", FloatType(), True),
    StructField("bank_interest", FloatType(), True)
])

JOINED_DATA_SCHEMA = [
    # transaction fields
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("transaction_id", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("amount", "FLOAT"),
    bigquery.SchemaField("total_amount", "FLOAT"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("merchant_id", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("deposit_method", "STRING"),
    bigquery.SchemaField("installment", "BOOLEAN"),
    bigquery.SchemaField("installment_amount", "FLOAT"),
    bigquery.SchemaField("bank_interest", "FLOAT"),

    # customer fields
    bigquery.SchemaField("age", "INTEGER"),
    bigquery.SchemaField("fullname", "STRING"),
    bigquery.SchemaField("job", "STRING"),
    bigquery.SchemaField("marital", "STRING"),
    bigquery.SchemaField("balance", "FLOAT"),
    bigquery.SchemaField("housing", "STRING"),
    bigquery.SchemaField("loan", "BOOLEAN"),
    bigquery.SchemaField("contact", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
    bigquery.SchemaField("credit_score", "INTEGER"),
    bigquery.SchemaField("geography", "STRING"),
    bigquery.SchemaField("number_of_products", "INTEGER"),
    bigquery.SchemaField("tenure", "INTEGER"),
    bigquery.SchemaField("loan_number", "INTEGER"),

    bigquery.SchemaField("upload_time", "TIMESTAMP"),
]


def main():
    create_dataset_if_not_exists(DATASET_NAME)

    partition_field = 'upload_time'
    partition_parameters = {
        'partition_field': partition_field,
        'partition_type': 'HOUR'
    }
    create_table_if_not_exists(
        DATASET_NAME, VALID_TABLE_NAME,
        JOINED_DATA_SCHEMA,
        partition_parameters=partition_parameters
    )

    create_table_if_not_exists(
        DATASET_NAME, INVALID_TABLE_NAME,
        JOINED_DATA_SCHEMA,
        partition_parameters=partition_parameters
    )

    customers_df = extract_data_from_gcs_to_spark_df(
        bucket_name=BUCKET_NAME,
        dir_name=CUSTOMER_DATA_PATH,
        spark_session=spark,
        schema=CUSTOMER_SCHEMA
    )
    transactions_df = extract_data_from_gcs_to_spark_df(
        bucket_name=BUCKET_NAME,
        dir_name=TRANSACTION_DATA_PATH,
        spark_session=spark,
        schema=TRANSACTION_SCHEMA
    )

    logger.info('Starting data transformation')

    transactions_df = transactions_df.dropDuplicates(
        ['transaction_id'])
    customers_df = customers_df.dropDuplicates(['customer_id'])

    transactions_df = transactions_df.withColumn('amount', col('amount').cast(
        DoubleType()))
    transactions_df = transactions_df.withColumn('total_amount',
                                                 col('total_amount').cast(
                                                     DoubleType()))

    # handling non-critical fields
    transactions_df = transactions_df.fillna({'city': 'unknown'})

    customers_df = customers_df.fillna({
        'job': 'unknown',
        'housing': 'unknown',
        'marital': 'unknown'
    })

    customers_df = customers_df.withColumn('gender', when(
        col('gender').isin('male', 'female', 'other'), col('gender')) \
                                           .otherwise('unknown'))

    # get list of valid currencies
    bq_client = bigquery.Client()
    query = """
        SELECT currency_code
        FROM `bank-transaction-processing-3.bank_dataset.currency_codes`
        """
    query_job = bq_client.query(query)
    results = query_job.result()
    valid_currencies = [row.currency_code for row in results]
    # valid_currencies = ['GEL', 'USD', 'EUR']

    customers_df = customers_df.repartition('customer_id')
    transactions_df = transactions_df.repartition('transaction_id')

    joined_df = transactions_df.join(customers_df, on='customer_id',
                                     how='inner')

    joined_df = joined_df.cache()

    logger.info(f'Joined df schema: {joined_df.printSchema()}')

    filter_parameters = (
            ~isnan(col('transaction_id')) &
            ~isnan(col('customer_id')) &
            ((col('amount') >= 0.5) & (col('total_amount') >= 0.5)) &
            col('currency').isin(valid_currencies) &
            ((col('age') >= 14) & (col('age') <= 100))
    )

    valid_data = joined_df.filter(filter_parameters).withColumn(
        'upload_time', current_timestamp()
    )

    invalid_data = joined_df.filter(~filter_parameters).withColumn(
        'upload_time', current_timestamp()
    )

    logger.info('Data transformation successful')

    load_spark_df_to_bq(valid_data, DATASET_NAME, VALID_TABLE_NAME,
                        DATAPROC_TEMP_BUCKET)
    load_spark_df_to_bq(invalid_data, DATASET_NAME, INVALID_TABLE_NAME,
                        DATAPROC_TEMP_BUCKET)


if __name__ == '__main__':
    main()
