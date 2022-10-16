from datetime import datetime 
from datetime import timedelta
import os
import sys 

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

sys.path.insert(0 , '/home/workspace/airflow/plugins')

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False

}#default_args

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data from s3 to Redshift with Airflow',
          schedule_interval='0 * * * *',# RUN every hour
        )#dag

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_person_to_redshift = StageToRedshiftOperator(
    task_id='Stage_person',
    dag=dag,
    table='staging_person',
    aws_connection_id='aws_credentials',
    redshift_connection_id='redshift',
    create_query="""
    CREATE TABLE IF NOT EXISTS staging_person(
    i94res FLOAT,
    i94port VARCHAR,
    i94mode FLOAT,
    i94cit FLOAT,
    i94mon FLOAT,
    arrdate VARCHAR,
    i94addr VARCHAR,
    dedate VARCHAR,
    i94bir FLOAT,
    i94visa FLOAT,
    occup VARCHAR,
    biryear FLOAT,
    gender VARCHAR,
    airline VARCHAR,
    adnum FLOAT,
    fltno VARCHAR
);
    """,
    s3_bucket='stagings33',
    s3_key='person/',
    copy_options="format as parquet;"
)
stage_Immigrante_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Immigrante',
    dag=dag,
    table='staging_Immigrante',
    aws_connection_id='aws_credentials',
    redshift_connection_id='redshift',
    create_query="""
    CREATE TABLE IF NOT EXISTS staging_Immigrante(
    cicid FLOAT,
    adnum FLOAT,
    i94cit FLOAT,
    i94port VARCHAR,
    arrdate VARCHAR,
    dedate VARCHAR
);
    """,
    s3_bucket='stagings33',
    s3_key='immigration/',
    copy_options="format as parquet;"
)
stage_airport_to_redshift = StageToRedshiftOperator(
    task_id='Stage_airport',
    dag=dag,
    table='staging_airport',
    aws_connection_id='aws_credentials',
    redshift_connection_id='redshift',
    create_query="""
    CREATE TABLE IF NOT EXISTS staging_airport(
    ID VARCHAR,
    type VARCHAR,
    name VARCHAR,
    iso_country VARCHAR,
    Municipality VARCHAR,
    iata_code VARCHAR,
    local_code VARCHAR
);
    """,
    s3_bucket='stagings33',
    s3_key='airport/',
    copy_options="format as parquet;"
)
stage_demographic_to_redshift = StageToRedshiftOperator(
    task_id='Stage_demographic',
    dag=dag,
    table='staging_demographic',
    aws_connection_id='aws_credentials',
    redshift_connection_id='redshift',
    create_query="""
    CREATE TABLE IF NOT EXISTS staging_demographic(
    city VARCHAR,
    state VARCHAR,
    Median_age FLOAT,
    male_population BIGINT,
    female_population BIGINT,
    total_male_population BIGINT,
    Number_of_ventreans BIGINT,
    foreign_born BIGINT,
    Average_Household_Size FLOAT,
    state_code VARCHAR,
    race VARCHAR,
    Count BIGINT
);
    """,
    s3_bucket='stagings33',
    s3_key='demographics/',
    copy_options="format as parquet;"
)

load_Immigrante_table = LoadFactOperator(
    task_id='Load_Immigrante_fact_table',
    dag=dag,
    conn_id='redshift',
    table='Immigrante_table',
    create_query="""
    CREATE TABLE IF NOT EXISTS public.Immigrante_table (
	cicid FLOAT PRIMARY KEY,
	adnum FLOAT,
	i94cit FLOAT,
	i94port VARCHAR,
	arrdate VARCHAR,
	dedate VARCHAR
);
    """,
    insert_query=SqlQueries.Immigrante_table_insert,
)



load_person_dimension_table = LoadDimensionOperator(
    task_id='Load_person_table_dim',
    dag=dag,
    conn_id='redshift',
    table='person_table_dim',
    create_query="""
    CREATE TABLE IF NOT EXISTS public.person_table_dim (
	i94res FLOAT,
	i94port VARCHAR,
	i94mode FLOAT,
	i94cit FLOAT,
	i94mon FLOAT,
	arrdate VARCHAR,
	i94addr VARCHAR,
	dedate VARCHAR,
	i94bir FLOAT,
	i94visa FLOAT,
	occup VARCHAR,
	biryear FLOAT,
	gender VARCHAR,
	airline VARCHAR,
	adnum FLOAT PRIMARY KEY,
	fltno VARCHAR
);
    """,
    insert_query=SqlQueries.person_table_insert,
    append = False
)

load_airport_dimension_table = LoadDimensionOperator(
    task_id='Load_airport_table_dim',
    dag=dag,
    conn_id='redshift',
    table='airport_table_dim',
    create_query="""
    CREATE TABLE IF NOT EXISTS public.airport_table_dim (
	ID VARCHAR PRIMARY KEY,
	type VARCHAR,
	name VARCHAR,
	iso_country VARCHAR,
	Municipality  VARCHAR,
	iata_code VARCHAR,
	local_code VARCHAR
);
    """,
    insert_query=SqlQueries.airport_table_insert,
    append = False
)

load_demographic_dimension_table = LoadDimensionOperator(
    task_id='Load_demographic_table_dim',
    dag=dag,
    conn_id='redshift',
    table='demographic_table_dim',
    create_query="""
    CREATE TABLE IF NOT EXISTS public.demographic_table_dim (
    city VARCHAR,
    state VARCHAR,
    Median_age FLOAT,
    male_population BIGINT,
    female_population BIGINT,
    total_male_population BIGINT,
    Number_of_ventreans BIGINT,
    foreign_born BIGINT,
    Average_Household_Size FLOAT,
    state_code VARCHAR,
    race VARCHAR,
    Count BIGINT
);
    """,
    insert_query=SqlQueries.demographic_table_insert,
    append = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id='redshift',
    tables=["Immigrante_table","airport_table_dim","person_table_dim", "demographic_table_dim"]
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_person_to_redshift
start_operator >> stage_Immigrante_to_redshift
start_operator >> stage_airport_to_redshift
start_operator >> stage_demographic_to_redshift


stage_person_to_redshift >> load_person_dimension_table
stage_Immigrante_to_redshift >> load_Immigrante_table
stage_airport_to_redshift >> load_airport_dimension_table
stage_demographic_to_redshift >> load_demographic_dimension_table

load_person_dimension_table >> run_quality_checks
load_Immigrante_table >> run_quality_checks
load_airport_dimension_table >> run_quality_checks
load_demographic_dimension_table >> run_quality_checks

run_quality_checks >> end_operator