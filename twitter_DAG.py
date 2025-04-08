from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_ETL import twitter_ETL

# default arguments for the DAG
default_args = {
    'owner': 'Aaditya',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 8),
    'end_date': datetime(2025, 4, 10),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# define the DAG
dag = DAG(
    'Twitter-ETL-DAG',
    default_args=default_args,
    description='Twitter ETL DAG',
)

# define the task
ETL_task = PythonOperator(
    task_id='twitter_ETL',
    python_callable=twitter_ETL,
    dag=dag,
)

# set the task dependencies
ETL_task