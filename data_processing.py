import os

os.environ['SPARK_HOME'] = "/Users/quanghuyphung15/Downloads/spark-3.5.1-bin-hadoop3"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ["HADOOP_USER_NAME"] = "hdfs"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, sum as _sum, date_format, to_date

# Define schema for danh_sach_sv_de
schema_sv = StructType([
    StructField(name="student_code", dataType=IntegerType(), nullable=True),
    StructField(name="student_name", dataType=StringType(), nullable=True),
])

# Define schema for log
schema_log = StructType([
    StructField(name="student_code", dataType=IntegerType(), nullable=True),
    StructField(name="activity", dataType=StringType(), nullable=True),
    StructField(name="numberOfFile", dataType=IntegerType(), nullable=True),
    StructField(name="timestamp", dataType=StringType(), nullable=True),
])

# Create Spark Session
spark = SparkSession.builder.master("local") \
                .appName("VDT2024 Assignment") \
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
                .config("spark.driver.memory", "16g")\
                .getOrCreate()

# Create DataFrames from CSV files
sv_df = spark.read.format("csv").load("hdfs://localhost:9870/raw_zone/fact/activity/danh_sach_sv_de.csv", header=False, schema=schema_sv)
log_df = spark.read.format("parquet").load("hdfs://localhost:9870/raw_zone/fact/activity", schema=schema_log)

# Process data
result_df = log_df.withColumn('date', date_format(to_date(col('timestamp'), "m/dd/yyyy"), "yyyymmdd")) \
    .groupBy('date', 'student_code', 'activity') \
    .agg(_sum('numberOfFile').alias('totalFile')) \
    .join(sv_df, on='student_code', how='inner') \
    .select('date', 'student_code', 'student_name', 'activity', 'totalFile') \
    .orderBy("date")

# Show the result
result_df.show()

# Save the result to a CSV file
result_df.write.format("csv").option("header", "true").mode("overwrite").save("./Tên_sinh_viên.csv")

# Stop the Spark session
spark.stop()