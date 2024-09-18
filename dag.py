from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, count, when

schema = StructType([
    StructField("email", StringType(), False),
    StructField("action", StringType(), False),
    StructField("date", TimestampType(), False)
])

def skip_if_catchup(**kwargs):
    if kwargs['execution_date'] < days_ago(0):
        return 'skip_weekly_report'
    return 'create_weekly_report'

def create_daily_report(date):
    spark = SparkSession.builder \
        .appName("Create daily report") \
        .getOrCreate()

    csv_file_path = f"/input/{date}.csv"

    df = spark.read.csv(
        csv_file_path,
        schema=schema,
        header=False,
        sep=","
    )

    counted_actions = df.groupBy("email").agg(
        count(when(col("action") == "create", 1)).alias("create_num"),
        count(when(col("action") == "read", 1)).alias("read_num"),
        count(when(col("action") == "update", 1)).alias("update_num"),
        count(when(col("action") == "delete", 1)).alias("delete_num")
    )

    output_path = f"/daily_reports/{date}.csv"

    counted_actions.write.csv(output_path, header=True, mode='overwrite')

    spark.stop()
    return 200

def create_weekly_report(date):
    return 200

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
        start_date=datetime.today() - timedelta(days=7),
) as dag:

    start = EmptyOperator(task_id = 'start')

    get_todays_file = BashOperator(
        task_id = 'get_todays_file',
        bash_command=(
            'python generate.py input {{ ds }} 1 10 2000'
        )
    )

    create_daily_report_task = PythonOperator(
        task_id='create_daily_report',
        python_callable=create_daily_report,
        op_kwargs={'date': business_dt}
    )
    
    skip_if_catchup_task = BranchPythonOperator(
        task_id='skip_if_catchup',
        python_callable=skip_if_catchup,
        provide_context=True
    )

    create_weekly_report_task = PythonOperator(
        task_id='create_weekly_report',
        python_callable=create_weekly_report,
        op_kwargs={'date': business_dt}
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
