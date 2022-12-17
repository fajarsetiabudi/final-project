from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator

#inisiasi dag
with DAG(
    "spark_etl",
    start_date = datetime(2022, 12, 10),
    schedule_interval = None
    ) as dag: 
    
    job_start = DummyOperator(
            task_id = "job_start"
    )
    
     # ETL data from csv to mysql
    csv_to_mysql = BashOperator(
    	task_id = 'csv_to_mysql',
    	bash_command='python3 /home/fajarsetia/Documents/de9-final-project-main/spark/app/main.py'
        )
    
     # ETL data from mysql to postgres
    mysql_to_postgres = BashOperator(
    	task_id = 'mysql_to_postgres',
    	bash_command='python3 /home/fajarsetia/Documents/de9-final-project-main/spark/app/mysql_to_postgres.py'
        )
    
    # Finish job
    job_finish = DummyOperator(
        task_id = "job_finish"
        )
    
     # Orchestration
    (
        job_start
        >> csv_to_mysql
        >> mysql_to_postgres
        >> job_finish
    )