import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.careerbuilder.extract_min_max_yoe import extract_min_max_yoe
from spark.careerbuilder.format_job_id import format_job_id
from spark.careerbuilder.modify_company_name import modify_company_name
from spark.careerbuilder.modify_employment_type import normalize_employment_type
from spark.careerbuilder.modify_job_address import modify_job_address
from spark.careerbuilder.modify_job_deadline import convert_to_job_deadline_datetime
from spark.careerbuilder.modify_job_listed import convert_to_job_listed_datetime
from spark.careerbuilder.modify_job_title import modify_job_title
from spark.careerbuilder.modify_salary import extract_min_max_salary
from spark.careerbuilder.normalize_industries import normalize_industries
from spark.careerbuilder.normalize_job_function import normalize_job_function

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "careerbuilder"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "job-analysis"
MONGO_COLLECTION_NAME = "careerbuilder"


def transform_and_ingest():
    # spark = SparkSession.builder \
    #     .appName("careerbuilder") \
    #     .config("spark.mongodb.input.uri", "mongodb://localhost:27017/hoangph34.careerbuilder") \
    #     .config("spark.mongodb.output.uri", "mongodb://localhost:27017/hoangph34.careerbuilder") \
    #     .getOrCreate()
    #
    # job_df = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    #     .option("subscribe", KAFKA_TOPIC) \
    #     .load()

    spark = SparkSession.builder.appName("example").getOrCreate()
    job_df = spark.read.format("json").option("header", "true").load("careerbuilder.json")

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

    job_df.write.format("mongo").mode("append").option("uri", MONGO_URI).option("database", MONGO_DB_NAME).option(
        "collection", MONGO_COLLECTION_NAME).save()

    # query = job_df.writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda batch_df, batch_id: batch_df.write \
    #                   .format("com.mongodb.spark.sql.DefaultSource") \
    #                   .mode("append") \
    #                   .option("uri", "mongodb://localhost:27017/hoangph34.careerbuilder") \
    #                   .option("database", "hoangph34") \
    #                   .option("collection", "careerbuilder") \
    #                   .save()) \
    #     .start()

    # Wait for the streaming to finish
    # query.awaitTermination()


if __name__ == "__main__":
    transform_and_ingest()
