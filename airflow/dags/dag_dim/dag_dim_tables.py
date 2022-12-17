from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago 


# DAG Definition
default_args = {
	'owner': 'Admin',
}

with DAG(
    "dim_tables",
    start_date = days_ago(1),
    schedule_interval = None,
    default_args = default_args
) as dag:

    # Start job
    job_start = DummyOperator(
        task_id = "job_start"
        )
    
    # Create dependencies from other dags
    with TaskGroup("other_dag_dependencies") as other_dag_dependencies:
        # dag mongodb
        mongodb_task = BaseSensorOperator(
            task_id = 'mongodb_task',
            external_dag_id='mongo_etl'
            )

        # dag spark
        spark_task = BaseSensorOperator(
            task_id = 'spark_task',
            external_dag_id='spark_etl'
            )


    # Run dim_country sql script
    create_dim_country = PostgresOperator(
        task_id = "create_dim_country",
        postgres_conn_id = "airflow_final_project",
        sql = "/home/fajarsetia/Documents/final_project_de9/dim_table/dim_country.sql"
    )

    # Run dim_state sql script
    create_dim_state = PostgresOperator(
        task_id = "create_dim_state",
        postgres_conn_id = "airflow_final_project",
        sql = "/home/fajarsetia/Documents/final_project_de9/dim_table/dim_state.sql"
    )

    # Run dim_city sql script
    create_dim_city = PostgresOperator(
        task_id = "create_dim_city",
        postgres_conn_id = "airflow_final_project",
        sql = "/home/fajarsetia/Documents/final_project_de9/dim_table/dim_city.sql"
    )

    # Run dim_currency sql script
    create_dim_currency = PostgresOperator(
        task_id = "create_dim_currency",
        postgres_conn_id = "airflow_final_project",
        sql = "/home/fajarsetia/Documents/final_project_de9/dim_table/dimension_table/dim_currency.sql"
    ) 
 
    # Finish job
    job_finish = DummyOperator(
        task_id = "job_finish"
        )


    # Orchestration
    (
        job_start
        >> other_dag_dependencies
        >> create_dim_country
        >> create_dim_state
        >> create_dim_city
        >> create_dim_currency
        >> job_finish
    )
