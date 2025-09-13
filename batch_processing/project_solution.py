"""
Airflow DAG for Batch Data Lake Pipeline
Orchestrates the multi-hop data processing pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'batch_data_lake_pipeline',
    default_args=default_args,
    description='Фінальний проєкт частина 2: Multi-hop Data Lake Pipeline for Olympic Athletes Data',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['data-lake', 'batch-processing', 'olympic-data', 'final-project'],
)

# Task 1: Landing to Bronze - Завантаження з FTP та конвертація в Parquet
landing_to_bronze_task = SparkSubmitOperator(
    task_id='landing_to_bronze',
    application='dags/batch_processing/landing_to_bronze.py',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
    application_args=[],
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true'
    }
)

# Task 2: Bronze to Silver - Очищення тексту та дедублікація
bronze_to_silver_task = SparkSubmitOperator(
    task_id='bronze_to_silver',
    application='dags/batch_processing/bronze_to_silver.py',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
    application_args=[],
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true'
    }
)

# Task 3: Silver to Gold - Агрегація та розрахунок статистик
silver_to_gold_task = SparkSubmitOperator(
    task_id='silver_to_gold',
    application='dags/batch_processing/silver_to_gold.py',
    conn_id='spark-default',
    verbose=1,
    dag=dag,
    application_args=[],
    conf={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true'
    }
)

# Define task dependencies - Послідовне виконання всіх трьох файлів
landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task