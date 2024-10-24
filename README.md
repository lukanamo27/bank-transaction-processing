# Bank Transaction Processing

This project implements an automated ETL pipeline for processing bank transactions using Airflow, Spark, and GCP
services.

## Features

- ETL pipeline for bank transaction data processing
- Data validation and cleaning
- Creation of data marts for analytics
- Automated cleanup of temporary data
- Integration with Google Cloud Platform services

## Prerequisites

- Python 3.8+
- Airflow
- Spark
- GCP account

## Setup Instructions

1. Clone the repository
2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Set up your GCP credentials

## Project Structure

```
bank_transaction_processing_2.0/
├── dags/                    
├── spark_jobs/              
├── tasks/                 
├── utils/
├── .env                            
└── requirements.txt                 
```
