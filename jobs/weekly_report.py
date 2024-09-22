import argparse
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import sum

parser = argparse.ArgumentParser(description='Create weekly report')
parser.add_argument('--date', required=True, help='Execution date in YYYY-MM-DD format')
args = parser.parse_args()

date = args.date

spark = SparkSession.builder \
    .appName("Create weekly report") \
    .getOrCreate()

a = [
    (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=i)).strftime('%Y-%m-%d')
    for i in range(7)
]

csv_file_paths = [
    f"/opt/airflow/daily_reports/{a[i]}.csv"
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
        header=True,
        sep=","
    )

    weekly_report = weekly_report.union(df)

weekly_report = weekly_report.groupBy("email").agg(
    sum("create_num").alias("create_num"),
    sum("read_num").alias("read_num"),
    sum("update_num").alias("update_num"),
    sum("delete_num").alias("delete_num")
)

output_path = f"/opt/airflow/output/{date}.csv"

df_pandas = weekly_report.toPandas()

df_pandas.to_csv(output_path, index=False)

spark.stop()