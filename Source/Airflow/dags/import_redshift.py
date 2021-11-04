"""Airflow DAG for staging and Extracting immigration data from data lake to data warehouse"""

from datetime import datetime
from airflow.models import DAG
from airflow.operators import StageToRedshiftOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from helpers import RedshiftSqlQuerie
from airflow.operators import DataQualityOperator


DATABASE_CONID = "capstone-redshift"
DL_PATH = "s3://msf-capstone"
AWS_CREDENTIALS_ID = "aws_credentials"

default_arguments = {
    'owner': 'udacity',
    'description': 'Import immigration staging data to various supporting dimension tables and fact table',
    'start_date': datetime(2016, 1, 1),
    'end_date': datetime(2016, 1, 31),

}

# Partitioned path broken down by year, month, and day or arrival
partition_path = \
    "{{execution_date.strftime('s3://msf-capstone/immigration_data/year=%Y/month=%-m/arrival_day=%-d/')}}"

my_dag = DAG('import_i94_redshift',
          default_args=default_arguments,
          schedule_interval='@daily',
          max_active_runs=1,
          concurrency=3)

# Staging immigration data for partition path to database
stage_immigration_data_task = StageToRedshiftOperator(
    task_id="stage_immigration_data",
    dag=my_dag,
    redshift_conn_id=DATABASE_CONNECTION_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_path=partition_path,
    tablename="staging_immigration",
    truncate_table=True,
    copy_format="PARQUET"
)


def extract_ports():
    """
    Extracting ports data from staging tables
    :return:
    """
    postgres_hk = PostgresHook(DATABASE_CONID)
    postgres_hk.run(RedshiftSqlQueries.extract_ports)


ext_ports_task = PythonOperator(
    task_id="extract_ports",
    dag=my_dag,
    python_callable=extract_ports
)


def extract_airports():
    """
    Extracting airport data from staging tables
    :return:
    """
    postgres_hk = PostgresHook(DATABASE_CONNECTION_ID)
    postgres_hk.run(RedshiftSqlQueries.extract_airports)


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
    postgres_hk.run(RedshiftSqlQueries.extract_demographics)


ext_demographics_task = PythonOperator(
    task_id="extract_demographics",
    dag=my_dag,
    python_callable=extract_demographics
)


def extract_timedata():
    """
    Extracting time data from staging tables
    :return:
    """
    postgres_hk = PostgresHook(DATABASE_CONID)
    postgres_hk.run(RedshiftSqlQueries.extract_timedata)


ext_time_data_task = PythonOperator(
    task_id="extract_time_data",
    dag=my_dag,
    python_callable=extract_time_data
)


def extract_countries():
    """
    Extracting countries data from staging tables
    """
    postgres_hk = PostgresHook(DATABASE_CONID)
    postgres_hk.run(RedshiftSqlQueries.extract_countries)


ext_countries_task = PythonOperator(
    task_id="extract_countries",
    dag=my_dag,
    python_callable=extract_countries
)


def extract_immigration_data():
    """
    Extracting immigration data from staging tables
    :return:
    """
    postgres_hk = PostgresHook(DATABASE_CONNECTION_ID)
    postgres_hk.run(RedshiftSqlQueries.extract_immigration_data)


ext_immigration_data_task = PythonOperator(
    task_id="extract_immigration_data",
    dag=my_dag,
    python_callable=extract_immigration_data
)

# Ensuring that fact table receives expected number of records
staging_to_fact_data_quality_check = DataQualityOperator(
    task_id='staging_to_fact_data_quality_check',
    dag=my_dag,
    conn_id=DATABASE_CONID,
    sql_check_query=RedshiftSqlQueries.staging_to_fact_data_quality_check,
    expected_results=lambda records_not_inserted: records_not_inserted == 0
)

# Ensuring that staging table receives at least 1 record
staging_count_data_quality_check = DataQualityOperator(
    task_id='staging_count_data_quality_check',
    dag=my_dag,
    conn_id=DATABASE_CONID,
    sql_check_query=RedshiftSqlQueries.staging_count_data_quality_check,
    expected_results=lambda records_inserted: records_inserted != 0
)

# Setup DAG pipeline
start_operator = DummyOperator(task_id='start_execution', dag=my_dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=my_dag)

start_operator >> stage_immigration_data_task >> staging_count_data_quality_check

staging_count_data_quality_check >> ext_ports_task
staging_count_data_quality_check >> ext_time_data_task >> ext_immigration_data_task
staging_count_data_quality_check >> ext_countries_task >> ext_immigration_data_task

ext_ports_task >> ext_airports_task >> ext_immigration_data_task
ext_ports_task >> ext_demographics_task >> ext_immigration_data_task

extract_immigration_data_task >> staging_to_fact_data_quality_check

staging_to_fact_data_quality_check >> end_operator
