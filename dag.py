from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import col, count, when

def skip_if_catchup(**kwargs):
    if kwargs['execution_date'] < days_ago(0):
        return 'skip_weekly_report'
    return 'create_weekly_report'

def create_daily_report(date):
    spark = SparkSession.builder \
        .appName("Create daily report") \
        .getOrCreate()

    csv_file_path = f"/input/{date}.csv"

    schema = StructType([
        StructField("email", StringType(), False),
        StructField("action", StringType(), False),
        StructField("date", TimestampType(), False)
    ])

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

    counted_actions.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

    spark.stop()
    return "Report generated successfully."

def create_weekly_report(date):
    spark = SparkSession.builder \
        .appName("Create weekly report") \
        .getOrCreate()

    csv_file_paths = [
        f"/daily_reports/{(datetime.strptime(date, "%Y-%m-%d") - timedelta(days=i)).strftime('%Y-%m-%d')}.csv"
        for i in range(7)
    ]

    schema = StructType([
        StructField("email", StringType(), True),
        StructField("create_num", IntegerType(), True),
        StructField("read_num", IntegerType(), True),
        StructField("update_num", IntegerType(), True),
        StructField("delete_num", IntegerType(), True)
    ])

    weekly_report = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    for csv_file_path in csv_file_paths:
        df = spark.read.csv(
            csv_file_path,
            schema=schema,
            header=False,
            sep=","
        )

        weekly_report = weekly_report.union(df)

    weekly_report = weekly_report.groupBy("email").agg(
        sum("create_num").alias("create_num"),
        sum("read_num").alias("read_num"),
        sum("update_num").alias("update_num"),
        sum("delete_num").alias("delete_num")
    )

    output_path = f"/output/{date}.csv"

    weekly_report.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

    spark.stop()
    return "Report generated successfully."

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
        schedule_interval='0 7 * * *',
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
