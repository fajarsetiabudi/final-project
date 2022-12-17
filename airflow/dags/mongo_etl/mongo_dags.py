from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator

#inisiasi dag
with DAG(
    "mongo_etl",
    start_date = datetime(2022, 12, 10),
    schedule_interval = None
    ) as dag: 
    
    job_start = DummyOperator(
            task_id = "job_start"
    )
    
     # ETL data from csv to mysql
    companies_mongo = BashOperator(
    	task_id = 'companies_mongo',
    	bash_command='python3 /home/fajarsetia/Documents/de9-final-project-main/mongo_etl/companies/companies.py'
        )
    
     # ETL data from mysql to postgres
    zips_mongo = BashOperator(
    	task_id = 'zips_mongo',
    	bash_command='python3 /home/fajarsetia/Documents/de9-final-project-main/spark/mongo_etl/zips/zips.py'
        )
    
    # Finish job
    job_finish = DummyOperator(
        task_id = "job_finish"
        )
    
     # Orchestration
    (
        job_start
        >> companies_mongo
        >> zips_mongo
        >> job_finish
    )