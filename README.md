# Bank Transaction Processing Pipeline

## Overview
This project implements an end-to-end data pipeline for processing bank transactions. It generates synthetic customer and transaction data, processes it through various stages, and ultimately makes it available for analysis in Looker.

## Architecture

The pipeline consists of the following components:

1. **Data Generation Layer**
   - Two Cloud Functions that generate synthetic data:
     - Customer data generator
     - Transaction data generator
   - Cloud Scheduler triggers these functions every minute

2. **Data Ingestion Layer**
   - Pub/Sub topics receive generated data
   - Dataflow jobs transfer data to Cloud Storage

3. **Data Processing Layer**
   - Three Airflow DAGs:
     - `bank_transactions_etl`: Processes raw data and loads it into BigQuery
     - `bank_data_marts_etl`: Creates analytical data marts
     - `cleanup_generated_data`: Performs cleanup operations

4. **Data Analysis Layer**
   - Looker connects to BigQuery for visualization and analysis

## Components

### Cloud Functions

#### Customer Generator
- Generates synthetic customer data using the Faker library
- Fields include: customer ID, age, name, job, marital status, balance, etc.
- Publishes data to a dedicated Pub/Sub topic

#### Transaction Generator
- Generates synthetic transaction data
- Pulls customer IDs from a Pub/Sub subscription
- Fields include: currency, amount, timestamp, merchant details, etc.
- Publishes data to a dedicated Pub/Sub topic

### Airflow DAGs

#### 1. Bank Transactions ETL
- Submits a Spark job to process raw data
- Performs data validation and transformation
- Separates valid and invalid records
- Loads data into BigQuery tables
- Triggers subsequent DAGs

#### 2. Bank Data Marts ETL
Creates three data marts:
- Transactions per customer
- Transactions per merchant
- Monthly transactions summary

#### 3. Cleanup Generated Data
- Removes processed data from temporary storage
- Maintains storage efficiency

### Data Validation
The pipeline includes several validation steps:
- Currency code verification against reference data
- Age validation (14-100 years)
- Transaction amount validation (minimum 0.5)
- Duplicate transaction removal
- Null handling for non-critical fields

## Setup Instructions

### Prerequisites
- Google Cloud Platform account
- Following APIs enabled:
  - Cloud Functions
  - Cloud Scheduler
  - Pub/Sub
  - Dataflow
  - Cloud Storage
  - BigQuery
  - Cloud Composer (Airflow)
- Python 3.8+

### Required Python Packages
```plaintext
functions-framework>=3.8.1
Faker>=30.8.1
google-cloud-pubsub
google-cloud-logging
google-cloud-storage
google-cloud-bigquery
apache-airflow
```

### Environment Variables
The following environment variables need to be set:
```plaintext
CUSTOMER_DATA_TOPIC_PATH
CUSTOMER_IDS_TOPIC_PATH
CUSTOMER_IDS_SUB_PATH
TRANSACTION_DATA_TOPIC_PATH
BUCKET_NAME
DATASET_NAME
DATAPROC_CLUSTER_NAME
DATAPROC_REGION
PATH_TO_PYSPARK_ETL_JOB
PATH_TO_PYSPARK_DATA_MARTS_JOB
CUSTOM_PACKAGE_URI
```

### Deployment Steps

1. **Deploy Cloud Functions**
   ```bash
   gcloud functions deploy generate_customers --runtime python39 --trigger-http
   gcloud functions deploy generate_transactions --runtime python39 --trigger-http
   ```

2. **Set up Cloud Scheduler**
   ```bash
   gcloud scheduler jobs create http generate-customers-job --schedule="* * * * *"
   gcloud scheduler jobs create http generate-transactions-job --schedule="* * * * *"
   ```

3. **Create Pub/Sub Topics and Subscriptions**
   ```bash
   gcloud pubsub topics create customer-data
   gcloud pubsub topics create transaction-data
   gcloud pubsub subscriptions create customer-data-sub --topic=customer-data
   ```

4. **Deploy Airflow DAGs**
   - Copy DAG files to your Cloud Composer environment
   - Verify DAGs appear in the Airflow UI

## Data Marts

### 1. Transactions Per Customer
Fields:
- customer_id
- num_transactions
- total_amount
- average_transaction_amount

### 2. Transactions Per Merchant
Fields:
- merchant_id
- num_transactions
- total_sales

### 3. Monthly Transactions
Fields:
- transaction_month
- num_transactions
- total_amount

## Monitoring and Logging
- Cloud Functions use Cloud Logging
- Spark jobs include detailed logging
- Airflow DAGs include task-level logging
- BigQuery includes query logging and audit logs
