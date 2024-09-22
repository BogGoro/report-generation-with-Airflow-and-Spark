import argparse
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, count, when

parser = argparse.ArgumentParser(description='Create daily report')
parser.add_argument('--date', required=True, help='Execution date in YYYY-MM-DD format')
args = parser.parse_args()

date = args.date

spark = SparkSession.builder \
    .appName("Create daily report") \
    .getOrCreate()

csv_file_path = f"/opt/airflow/input/{date}.csv"

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
    count(when(col("action") == "CREATE", 1)).alias("create_count"),
    count(when(col("action") == "READ", 1)).alias("read_count"),
    count(when(col("action") == "UPDATE", 1)).alias("update_count"),
    count(when(col("action") == "DELETE", 1)).alias("delete_count")
)

output_path = f"/opt/airflow/daily_reports/{date}.csv"

df_pandas = counted_actions.toPandas()

df_pandas.to_csv(output_path, index=False)

spark.stop()