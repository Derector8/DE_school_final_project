import sys
sys.path.append("/opt/airflow/")
sys.path.append("/opt/airflow/scripts")

import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import scripts.views_scripts_sql as views


default_args: dict = dict({
    'owner' : 'airflow',
    'depend_on_past' : False,
    'start_date' : datetime.datetime(2024, 7, 8),
    'email': 'bucharevroman@gmail.com',
    'email_on_failure': True,
})

with DAG(
    'create_views_dag',
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
    description = 
    """
    !!! Start only when you ingested fact and dimentional tables postgres !!!
    This dag will create all views, listed in project tasks .
    """
    ) as dag:
    
    start_op = EmptyOperator(task_id = 'start_dag', dag = dag)
    
    creating_avg_sim_per_source_molecule_view_op =PostgresOperator(
        task_id = 'creating_avg_sim_per_source_molecule_view', 
        sql = views.avg_sim_per_source_molecule_view,
        postgres_conn_id = 'aws_postgres',
        autocommit = True, 
        dag = dag)
    
    creating_avg_deviation_of_alogp_view_op =PostgresOperator(
        task_id = 'creating_avg_deviation_of_alogp_view', 
        sql = views.avg_deviation_of_alogp_view,
        postgres_conn_id = 'aws_postgres',
        autocommit = True, 
        dag = dag)
    
    creating_pivot_10_source_molecules_view_op =PostgresOperator(
        task_id = 'creating_pivot_10_source_molecules_view', 
        sql = views.pivot_view,
        postgres_conn_id = 'aws_postgres',
        autocommit = True, 
        dag = dag)
    
    creating_next_and_second_most_sim_view_op =PostgresOperator(
        task_id = 'creating_next_and_second_most_sim_view', 
        sql = views.next_and_second_most_sim_view,
        postgres_conn_id = 'aws_postgres',
        autocommit = True, 
        dag = dag)
    
    creating_avg_similarity_score_total_view_op =PostgresOperator(
        task_id = 'creating_avg_similarity_score_total_view', 
        sql = views.avg_similarity_score_total_view,
        postgres_conn_id = 'aws_postgres',
        autocommit = True, 
        dag = dag)
    
    end_op = EmptyOperator(task_id = 'end_dag', dag = dag)


    start_op >> [
                creating_avg_sim_per_source_molecule_view_op, 
                creating_avg_deviation_of_alogp_view_op,
                creating_pivot_10_source_molecules_view_op,
                creating_next_and_second_most_sim_view_op,
                creating_avg_similarity_score_total_view_op,
                                                            ] >> end_op  