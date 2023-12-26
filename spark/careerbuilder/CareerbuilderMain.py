import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json


from extract_min_max_yoe import extract_min_max_yoe
from format_job_id import format_job_id
from modify_company_name import modify_company_name
from modify_employment_type import normalize_employment_type
from modify_job_address import modify_job_address
from modify_job_deadline import convert_to_job_deadline_datetime
from modify_job_listed import convert_to_job_listed_datetime
from modify_job_title import modify_job_title
from modify_salary import extract_min_max_salary
from normalize_industries import normalize_industries
from normalize_job_function import normalize_job_function
from careerbuilder_schema import job_schema

scala_version = '2.12'
spark_version = '3.2.3'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.5.0',
    'org.elasticsearch:elasticsearch-spark-30_2.12:7.17.16',
    "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
]

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "careerbuilder"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "job-analysis"
MONGO_COLLECTION_NAME = "careerbuilder"

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def transform_and_ingest():
    spark = SparkSession.builder \
        .appName("careerbuilder") \
        .master("local[*]") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.executor.heartbeatInterval", "10000ms") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/job-analysis.careerbuilder") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/job-analysis.careerbuilder") \
        .config("spark.cores.max", "2") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    job_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    job_df = job_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", job_schema).alias('data'))
    job_df = job_df.select('data.*')

    job_df.printSchema()

    job_df = job_df.withColumn("job_id", format_job_id(job_df["job_id"]))
    job_df = job_df.withColumn("job_title", modify_job_title(job_df["job_title"]))
    job_df = job_df.withColumn("job_listed", convert_to_job_listed_datetime(job_df["job_listed"]))
    job_df = job_df.withColumn("job_deadline", convert_to_job_deadline_datetime(job_df["job_deadline"]))
    job_df = extract_min_max_salary(job_df, "salary", "salary_min", "salary_max")
    job_df = job_df.withColumn("company_name", modify_company_name(job_df["company_name"]))
    job_df = job_df.withColumn("job_address", modify_job_address(job_df["job_address"]))
    job_df = extract_min_max_yoe(job_df, "job_experience_required", "job_yoe_min", "job_yoe_max")
    job_df = job_df.withColumn("employment_type", normalize_employment_type(job_df["employment_type"]))
    job_df = job_df.withColumn("job_level", normalize_job_function(job_df["job_function"])).drop("job_function")
    job_df = job_df.withColumn('industry', normalize_industries(job_df["industries"])).drop("industries")
    job_df = job_df.withColumn("ingested_at", F.current_date())
    job_df = job_df.withColumn("updated_at", F.current_date())

    query = job_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_mongodb) \
        .start()

    query.awaitTermination()


def write_to_mongodb(df, epoch_id):
    df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .mode("append") \
        .option("replaceDocument", "false") \
        .option("upsert", "true") \
        .option("database", "job-analysis") \
        .option("collection", "careerbuilder") \
        .save()


if __name__ == "__main__":
    transform_and_ingest()
