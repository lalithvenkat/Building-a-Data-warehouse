"""Apache Airflow DAG to setup database schema in Postgres database and import static staging data"""

import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from helpers import RedshiftSqlQueries

DL_PATH = "s3://msf-capstone"
DATABASE_CONID = "capstone-redshift"
AWS_CREDENTIALS_ID = "aws_credentials"

default_args = {"owner": "udacity"}

my_dag = DAG('setup_redshift',
          default_args=default_args,
          description='Create table schema in database and load static staging data',
          schedule_interval=None,
          start_date=datetime.datetime(2019, 1, 1),
          concurrency=3
          )

# Creating staging table for immigration data
create_immigration_table = PostgresOperator(
    task_id="create_staging_immigration_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_immigration_table
)

# Creating staging table for country data
create_countries_table = PostgresOperator(
    task_id="create_countries_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_countries_table
)

# Create staging table fot port data
create_stg_ports_table = PostgresOperator(
    task_id="create_stg_ports_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_stg_ports_table
)

# Creating staging table for airport data
create_stg_airports_table = PostgresOperator(
    task_id="create_stg_airports_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_stg_airports_table
)

# Creating staging table for demographics data
create_stg_demographics_table = PostgresOperator(
    task_id="create_stg_demographics_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_stg_demographics_table
)

# Creating dimension table for country data
create_countries_table = PostgresOperator(
    task_id="create_countries_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_countries_table
)

# Create dimension table for port data
create_ports_table = PostgresOperator(
    task_id="create_ports_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_ports_table
)

# Create dimension table for airport data
create_airports_table = PostgresOperator(
    task_id="create_airports_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_airports_table
)

# Create dimension table for demographics data
create_demographics_table = PostgresOperator(
    task_id="create_demographics_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_demographics_table
)

# Create dimension table for time data
create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_time_table
)

# Create fact table for immigration data
create_fact_img_table = PostgresOperator(
    task_id="fact_img_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_fact_img_table
)

# Creating city temperatures staging table
create_stg_city_temps_table = PostgresOperator(
    task_id="staging_city_temps_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_stg_city_temps_table
)

# Creating country temperatures staging table
create_stg_country_temps_table = PostgresOperator(
    task_id="staging_country_temps_table",
    dag=my_dag,
    postgres_conn_id=DATABASE_CONID,
    sql=RedshiftSqlQueries.create_stg_country_temp_table
)

# Loading port data into staging table
stage_port_codes_task = StageToRedshiftOperator(
    task_id="stage_port_codes",
    redshift_conn_id=DATABASE_CONID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    dag=my_dag,
    table_name="public.staging_ports",
    s3_path=f"{DL_PATH}/ports.csv",
    copy_format='CSV IGNOREHEADER 1',
    truncate_table=True
)

# Loading country codes into staging table
stage_country_codes_task = StageToRedshiftOperator(
    task_id="country_codes",
    dag=my_dag,
    redshift_conn_id=DATABASE_CONID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table_name='staging_countries',
    s3_path=f'{DL_PATH}/countries.csv',
    copy_format='CSV IGNOREHEADER 1',
    truncate_table=False,
)

# Loading airport data into staging table
st_airport_codes_task = StageToRedshiftOperator(
    task_id="airports_codes",
    redshift_conn_id=DATABASE_CONID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    dag=my_dag,
    table_name="staging_airports",
    s3_path=f"{DL_PATH}/airport-codes.csv",
    copy_format='CSV IGNOREHEADER 1',
    truncate_table=True
)

# Load demographics data into staging table
stg_demographics_task = StageToRedshiftOperator(
    task_id="stage_demographics",
    redshift_conn_id=DATABASE_CONID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    dag=my_dag,
    table_name="staging_demographics",
    s3_path=f"{DL_PATH}/cities-demographics.csv",
    truncate_table=True,
    copy_format="CSV DELIMITER ';' IGNOREHEADER 1",
)

#  Loading city temperature data into staging table
stg_city_temperatures_task = StageToRedshiftOperator(
    task_id="city_temperatures",
    dag=my_dag,
    redshift_conn_id=DATABASE_CONID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_path=f"{DATA_LAKE_PATH}/city_temperature_data/*.parquet",
    table_name="stg_city_temperatures",
    copy_format="PARQUET",
    truncate_table=True
)

#  Loading country temperature data into staging table
stg_country_temperatures_task = StageToRedshiftOperator(
    task_id="country_temperatures",
    dag=my_dag,
    redshift_conn_id=DATABASE_CONID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    s3_path=f"{DATA_LAKE_PATH}/country_temperature_data/*.parquet",
    table_name="stg_country_temperatures",
    copy_format="PARQUET",
    truncate_table=True
)


# Setup DAG pipeline
start_operator = DummyOperator(task_id='Begin_execution', dag=my_dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=my_dag)

start_operator >> create_immigration_table
start_operator >> create_countries_table
start_operator >> create_stg_ports_table
start_operator >> create_stg_airports_table
start_operator >> create_stg_demographics_table
start_operator >> create_time_table
start_operator >> create_ports_table
start_operator >> create_countries_table
start_operator >> create_stg_city_temps_table
start_operator >> create_stg_country_temps_table

create_ports_table >> create_airports_table
create_ports_table >> create_demographics_table

create_stg_countries_table >> stg_country_codes_task
create_stg_ports_table >> stg_port_codes_task
create_stg_airports_table >> stg_airports_codes_task
create_stg_demographics_table >> stg_demographics_task

create_time_table >> create_fact_img_table
create_ports_table >> create_fact_img_table
create_countries_table >> create_fact_img_table

create_stg_city_temps_table >> stg_city_temperatures_task
create_stg_country_temps_table >> stg_country_temperatures_task

create_stg_immigration_table >> end_operator
stg_country_codes_task >> end_operator
stg_port_codes_task >> end_operator
stg_airports_codes_task >> end_operator
stg_demographics_task >> end_operator
stg_city_temperatures_task >> end_operator
stg_country_temperatures_task >> end_operator
