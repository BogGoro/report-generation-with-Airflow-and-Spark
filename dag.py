import time
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def skip_if_catchup(**kwargs):
    if kwargs['execution_date'] < days_ago(0):
        return 'skip_weekly_report'
    return 'create_weekly_report'

def create_dayly_report(date):
    return 

def create_weekly_report(date):
    return 

args = {
    "owner": "BogGoro",
    'email': ['troyegubov.den@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'CRUD_weekly',
        default_args=args,
        description='Weekly report generation',
        schedule_interval='@daily',
        catchup=True,
        start_date=datetime.today() - timedelta(),
) as dag:

    start = EmptyOperator(task_id = 'start')

    get_todays_file = BashOperator(
        task_id = 'get_todays_file',
        bash_command=(
            'python generate.py input {{ ds }} 1 10 2000'
        )
    )

    create_dayly_report = PythonOperator(
        task_id='create_dayly_report',
        python_callable=create_dayly_report,
        op_kwargs={'date': business_dt}
    )
    
    skip_if_catchup = BranchPythonOperator(
        task_id='skip_if_catchup',
        python_callable=skip_if_catchup,
        provide_context=True
    )

    create_weekly_report = PythonOperator(
        task_id='create_weekly_report',
        python_callable=create_weekly_report,
        op_kwargs={'date': business_dt}
    )

    skip_weekly_report = EmptyOperator(task_id = 'skip_weekly_report')

    end = EmptyOperator(task_id = 'end')

    (
        start
        >> get_todays_file
        >> create_dayly_report
        >> skip_if_catchup
        >> [create_weekly_report, skip_weekly_report]
        >> end
    )
