import sys
sys.path.append("/opt/airflow/")
sys.path.append("/opt/airflow/scripts")

import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts import sql_scripts
from scripts import fingerprints_compute
from scripts import simularity_compute

default_args: dict = dict({
    'owner' : 'airflow',
    'depend_on_past' : False,
    'start_date' : datetime.datetime(2024, 7, 8),
    'email': 'bucharevroman@gmail.com',
    'email_on_failure': True,
})

with DAG(
    'compute_fps_and_sim_dag',
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
    description = 
    """
    !!! Start only when you ingested all ChemBL data to postgres !!!
    This dag will: 
    1) Compute fingerprints for all ChemBL molecules from postgres db and save them to postgres staging_fingerprints table and to s3 bucket. 
    2) Create fact table with top 10 molecule similarities and dimentional table with refered molecule properties.
    3) Compute Tanimoto similarity scores for input molecules with all other ChemBL molecules and save to s3 bucket.
    4) Find 10 most similar molecules for every input molecule and save to postgres fact table.
    5) Ingest dimentional table with molecule properties refered to fact table.
    """
    ) as dag:
    
    start_op = EmptyOperator(task_id = 'start_dag', dag = dag)

    creating_molecules_fingerprints_table_op =PostgresOperator(
        task_id = 'creating_molecules_fingerprints_table', 
        sql = sql_scripts.create_molecules_fingerprints_table,
        postgres_conn_id = 'aws_postgres',
        autocommit = True, 
        dag = dag)

    calculating_fingerprints_op = PythonOperator(
        task_id = 'calculating_fingerprints',
        python_callable = fingerprints_compute.run,
        dag = dag)
    
    creating_fact_top10_similarities_table_op =PostgresOperator(
        task_id = 'creating_fact_top10_similarities_table', 
        sql = sql_scripts.create_fact_molecule_similarities_table,
        postgres_conn_id = 'aws_postgres',
        autocommit = True, 
        dag = dag)
    
    creating_dim_top10_molecule_properties_table_op = PostgresOperator(
        task_id = 'creating_dim_top10_molecule_properties_table', 
        sql = sql_scripts.create_dim_molecule_properties_table,
        postgres_conn_id = 'aws_postgres',
        autocommit = True, 
        dag = dag)
    
    computing_similarity_scores_for_input_files_op = PythonOperator(
        task_id = 'calculate_and_save_fingerprints',
        python_callable = simularity_compute.compute_similarity,
        dag = dag)
    
    ingesting_dim_top10_molecule_properties_table_op = PostgresOperator(
        task_id = 'ingesting_dim_top10_molecule_properties_table', 
        sql = sql_scripts.ingest_dim_molecule_properties_sql,
        postgres_conn_id = 'aws_postgres',
        autocommit = True, 
        dag = dag)
    
    end_op = EmptyOperator(task_id = 'end_dag', dag = dag)


    
    start_op >> creating_molecules_fingerprints_table_op >> calculating_fingerprints_op

    calculating_fingerprints_op >> [creating_fact_top10_similarities_table_op, creating_dim_top10_molecule_properties_table_op] >> computing_similarity_scores_for_input_files_op
    
    computing_similarity_scores_for_input_files_op >> ingesting_dim_top10_molecule_properties_table_op >> end_op

    