import argparse
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
    count(when(col("action") == "create", 1)).alias("create_num"),
    count(when(col("action") == "read", 1)).alias("read_num"),
    count(when(col("action") == "update", 1)).alias("update_num"),
    count(when(col("action") == "delete", 1)).alias("delete_num")
)

output_path = f"/opt/airflow/daily_reports/{date}.csv"

counted_actions.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

spark.stop()