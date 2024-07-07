import sys
sys.path.append("/opt/airflow/")
sys.path.append("/opt/airflow/scripts")

import requests
import datetime

import pandas as pd
from airflow import DAG
from airflow.settings import Session
from airflow.utils.db import provide_session
from airflow.models import (
    Connection,
    Variable,
    )
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

import scripts.credentials as cr
Variable.set(key="secret_aws_s3_key_id", value=cr.AWS_S3_CREDENTIALS["key"])
Variable.set(key="secret_aws_s3_access_key", value=cr.AWS_S3_CREDENTIALS["secret"])
Variable.set(key="secret_aws_s3_token", value=cr.AWS_S3_CREDENTIALS["token"])
Variable.set(key="secret_aws_postgres_engine", value=cr.AWS_POSTGRES_URI)
import scripts.database_ingestion as ingesting
from scripts import sql_scripts


default_args: dict = dict({
    'owner' : 'airflow',
    'depend_on_past' : False,
    'start_date' : datetime.datetime(2024, 7, 8),
    'email': 'bucharevroman@gmail.com',
    'email_on_failure': True,
})



aws_postgres_conn: Connection = Connection(conn_id="aws_postgres",
                                    conn_type="postgresql",
                                    host=cr.POSTGRES_HOST,
                                    login=cr.POSTGRES_LOGIN,
                                    password=cr.POSTGRES_PASSWORD,
                                    schema=cr.POSTGRES_DATABASE,
                                    port=cr.POSTGRES_PORT,)

session = Session() 
session.add(aws_postgres_conn)
session.commit()



with DAG(
    'ingest_database_dag',
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
    ) as dag:
    
    start_op = EmptyOperator(task_id = 'start_dag', dag = dag)

    creating_staging_chembl_id_lookup_table_op = PostgresOperator(task_id = 'creating_staging_chembl_id_lookup_table', 
                                       sql = sql_scripts.create_staging_chembl_id_lookup_table,
                                       postgres_conn_id = 'aws_postgres',
                                       autocommit = True, 
                                       dag = dag,
                                       )
    
    creating_staging_molecule_table_op = PostgresOperator(task_id = 'creating_staging_molecule_table', 
                                       sql = sql_scripts.create_staging_molecule_table,
                                       postgres_conn_id = 'aws_postgres',
                                       autocommit = True, 
                                       dag = dag,
                                       )
    
    link_1_op = EmptyOperator(task_id = 'Link_1', dag = dag)

    ingesting_staging_chembl_id_lookup_table_op = PythonOperator(task_id = 'ingesting_staging_chembl_id_lookup_table',
                            python_callable = ingesting.run_multiprocessor,
                            op_kwargs = {"mode":"chembl_id_lookup", "multiplier":100},
                            dag = dag,
                            )
    
    ingesting_staging_molecule_table_op = PythonOperator(task_id = 'ingesting_staging_molecule_table',
                            python_callable = ingesting.run_multiprocessor,
                            op_kwargs = {"multiplier":5},
                            dag = dag,
                            )
    
    link_2_op = EmptyOperator(task_id = 'Link_2', dag = dag)

    creating_compound_properties_table_op = PostgresOperator(task_id = 'creating_compound_properties_table', 
                                       sql = sql_scripts.create_compound_properties_table,
                                       postgres_conn_id = 'aws_postgres',
                                       autocommit = True, 
                                       dag = dag,
                                       )
    
    creating_compound_structures_table_op = PostgresOperator(task_id = 'creating_compound_structures_table', 
                                       sql = sql_scripts.create_compound_structures_table,
                                       postgres_conn_id = 'aws_postgres',
                                       autocommit = True, 
                                       dag = dag,
                                       )

    creating_molecules_dictionary_table_op = PostgresOperator(task_id = 'creating_molecules_dictionary_table', 
                                       sql = sql_scripts.create_molecules_dict_table,
                                       postgres_conn_id = 'aws_postgres',
                                       autocommit = True, 
                                       dag = dag,
                                       )

    creating_chembl_id_lookup_table_op = PostgresOperator(task_id = 'creating_chembl_id_lookup_table', 
                                       sql = sql_scripts.create_chembl_id_lookup_table,
                                       postgres_conn_id = 'aws_postgres',
                                       autocommit = True, 
                                       dag = dag,
                                       )
    
    
    ingesting_compound_properties_table_op = PostgresOperator(task_id = 'ingesting_compound_properties_table', 
                                       sql = sql_scripts.ingest_compound_properties_sql,
                                       postgres_conn_id = 'aws_postgres',
                                       autocommit = True, 
                                       dag = dag,
                                       )
    
    ingesting_compound_structures_table_op = PostgresOperator(task_id = 'ingesting_compound_structures_table', 
                                       sql = sql_scripts.ingest_compound_structures_sql,
                                       postgres_conn_id = 'aws_postgres',
                                       autocommit = True, 
                                       dag = dag,
                                       )

    ingesting_molecules_dictionary_table_op = PostgresOperator(task_id = 'ingesting_molecules_dictionary_table', 
                                       sql = sql_scripts.ingest_molecules_dict_sql,
                                       postgres_conn_id = 'aws_postgres',
                                       autocommit = True, 
                                       dag = dag,
                                       )

    ingesting_chembl_id_lookup_table_op = PostgresOperator(task_id = 'ingesting_chembl_id_lookup_table', 
                                       sql = sql_scripts.ingest_chembl_id_lookup_sql,
                                       postgres_conn_id = 'aws_postgres',
                                       autocommit = True, 
                                       dag = dag,
                                       )
    
    link_3_op = EmptyOperator(task_id = 'Link_3',trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag = dag)
    
    end_op = EmptyOperator(task_id = 'end_dag', dag = dag)



    start_op >> [creating_staging_chembl_id_lookup_table_op, creating_staging_molecule_table_op] >> link_1_op

    link_1_op >> [ingesting_staging_chembl_id_lookup_table_op, ingesting_staging_molecule_table_op] >> link_2_op

    link_2_op >> creating_chembl_id_lookup_table_op >> ingesting_chembl_id_lookup_table_op >> link_3_op
    link_2_op >> creating_compound_properties_table_op >> ingesting_compound_properties_table_op >> link_3_op
    link_2_op >> creating_compound_structures_table_op >> ingesting_compound_structures_table_op >> link_3_op
    link_2_op >> creating_molecules_dictionary_table_op >> ingesting_molecules_dictionary_table_op >> link_3_op

    link_3_op >> end_op



