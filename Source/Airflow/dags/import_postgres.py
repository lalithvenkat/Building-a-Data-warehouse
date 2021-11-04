"""Airflow DAG for staging and extracting immigration data from data lake to data warehouse"""

from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from helpers import PostgresSqlQueries
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import StageParquetToPostgresOperator
from airflow.operators import DataQualityOperator
from airflow.models import DAG



DATABASE_CONID = "capstone-postgres"
DL_PATH = "/data/datalake"

default_arguments = {
    'owner': 'udacity',
    'description': 'Import immigration staging data to various supporting dimension tables and fact table',
    'start_date': datetime(2016, 5, 1),
    'end_date': datetime(2016, 5, 3),
}

# breaking down the partitioned path by year, month, and day or arrival
partition_path = \
    "{{execution_date.strftime('/data/datalake/immigration_data/year=%Y/month=%-m/arrival_day=%-d')}}"

my_dag = DAG('import_postgres',
          default_args=default_arguments,
          schedule_interval='@daily',
          concurrency=3,
          max_active_runs=1)

# Stage immigration data for partition path to database
stage_immigration_data_task = StageParquetToPostgresOperator(
    task_id="stage_immigration_data",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    parquet_path=partition_path,
    table_name="staging_immigration",
    truncate_table=True
)


def extract_ports():
    """
    Extracting ports data from staging tables
    :return:
    """
    postgres_hk = PostgresHook(DATABASE_CONID)
    postgres_hk.run(PostgresSqlQueries.extract_ports)


ext_ports_task = PythonOperator(
    task_id="extract_ports",
    dag=my_dag,
    python_callable=extract_ports
)


def extract_airports():
    """
    extracting airport data from staging tables
    :return:
    """
    postgres_hk = PostgresHook(DATABASE_CONID)
    postgres_hk.run(PostgresSqlQueries.extract_airports)


ext_airports_task = PythonOperator(
    task_id="extract_airports",
    dag=my_dag,
    python_callable=extract_airports
)


def extract_demographics():
    """
    Extracting demographics data from staging tables
    :return:
    """
    postgres_hk = PostgresHook(DATABASE_CONNECTION_ID)
    postgres_hk.run(PostgresSqlQueries.extract_demographics)


ext_demographics_task = PythonOperator(
    task_id="extract_demographics",
    dag=my_dag,
    python_callable=extract_demographics
)


def extract_timedata():
    """
    extracting time data from staging tables
    :return:
    """
    postgres_hk = PostgresHook(DATABASE_CONID)
    postgres_hk.run(PostgresSqlQueries.extract_time_data)


extract_time = PythonOperator(
    task_id="extract_time_data",
    dag=my_dag,
    python_callable=extract_timedata
)


def extract_countries():
    """
    extracting countries data from staging tables
    """
    postgres_hk = PostgresHook(DATABASE_CONNECTION_ID)
    postgres_hk.run(PostgresSqlQueries.extract_countries)


ext_countries_task = PythonOperator(
    task_id="extract_countries",
    dag=my_dag,
    python_callable=extract_countries
)


def extract_img_data():
    """
    Extracting immigration data from staging tables
    :return:
    """
    postgres_hk = PostgresHook(DATABASE_CONID)
    postgres_hk.run(PostgresSqlQueries.extract_img_data)


ext_immigration_data = PythonOperator(
    task_id="extract_immigration_data",
    dag=my_dag,
    python_callable=extract_immigration_data
)

# Ensuring that fact table receives the expected number of records
staging_to_fact_data_quality_check = DataQualityOperator(
    task_id='staging_to_fact_data_quality_check',
    dag=my_dag,
    conn_id=DATABASE_CONID,
    sql_check_query=PostgresSqlQueries.staging_to_fact_data_quality_check,
    expected_results=lambda records_not_inserted: records_not_inserted == 0
)

# Ensuring that staging table receives at least 1 record
staging_count_data_quality_check = DataQualityOperator(
    task_id='staging_count_data_quality_check',
    dag=my_dag,
    conn_id=DATABASE_CONID,
    sql_check_query=PostgresSqlQueries.staging_count_data_quality_check,
    expected_results=lambda records_inserted: records_inserted != 0
)

# Setting up DAG pipeline
start_operator = DummyOperator(task_id='start_execution', dag=my_dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=my_dag)

start_operator >> stage_immigration_data_task >> staging_count_data_quality_check

staging_count_data_quality_check >> ext_ports_task
staging_count_data_quality_check >> ext_time_data_task >> ext_immigration_data
staging_count_data_quality_check >> ext_countries_task >> ext_immigration_data

ext_ports_task >> ext_airports_task >> ext_immigration_data
ext_ports_task >> ext_demographics_task >> ext_immigration_data

ext_immigration_data >> staging_to_fact_data_quality_check

staging_to_fact_data_quality_check >> end_operator
