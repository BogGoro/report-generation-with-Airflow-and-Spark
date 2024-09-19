from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def skip_if_catchup(**kwargs):
    if kwargs['execution_date'] < days_ago(0):
        return 'skip_weekly_report'
    return 'create_weekly_report'

args = {
    "owner": "Denis Troegubov",
    'email': ['troyegubov.den@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
        'CRUD_weekly',
        default_args=args,
        description='Weekly report generation',
        schedule_interval='0 7 * * *',
        catchup=True,
        start_date=days_ago(6),
) as dag:

    start = EmptyOperator(task_id = 'start')

    get_todays_file = BashOperator(
        task_id = 'get_todays_file',
        bash_command=(
            'python /opt/airflow/generate/generate.py /opt/airflow/input {{ ds }} 1 10 2000'
        )
    )

    create_daily_report_task = SparkSubmitOperator(
        task_id='create_daily_report',
        conn_id='spark-conn',
        application='jobs/daily_report.py',
        application_args=['--date', '{{ ds }}']
    )
    
    skip_if_catchup_task = BranchPythonOperator(
        task_id='skip_if_catchup',
        python_callable=skip_if_catchup,
        provide_context=True
    )

    create_weekly_report_task = SparkSubmitOperator(
        task_id='create_weekly_report',
        conn_id='spark-conn',
        application='jobs/weekly_report.py',
        application_args=['--date', '{{ ds }}']
    )

    skip_weekly_report = EmptyOperator(task_id = 'skip_weekly_report')

    end = EmptyOperator(task_id = 'end')

    (
        start
        >> get_todays_file
        >> create_daily_report_task
        >> skip_if_catchup_task
        >> [create_weekly_report_task, skip_weekly_report]
        >> end
    )
