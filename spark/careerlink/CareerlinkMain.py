from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.careerlink.convert_job_listed_to_datetime import convert_to_job_listed_datetime
from spark.careerlink.extract_job_address import extract_job_address
from spark.careerlink.extract_job_deadline_date import extract_job_deadline_date
from spark.careerlink.extract_min_max_salary import extract_min_max_salary
from spark.careerlink.extract_min_max_yoe import extract_min_max_yoe
from spark.careerlink.modify_company_name import modify_company_name
from spark.careerlink.modify_job_title import modify_job_title
from spark.careerlink.normalize_employment_type import normalize_employment_type
from spark.careerlink.normalize_industries_Hoang_fix import normalize_industries
from spark.careerlink.normalize_job_function import normalize_job_function

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "careerbuilder"


def transform_and_ingest():
    spark = SparkSession.builder \
        .appName("careerbuilder") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/hoangph34.careerbuilder") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/hoangph34.careerbuilder") \
        .getOrCreate()

    job_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    job_df = job_df.withColumn("company_name", modify_company_name(job_df["company_name"]))
    job_df = job_df.withColumn("job_title", modify_job_title(job_df["job_title"]))
    job_df = job_df.withColumn("job_listed", convert_to_job_listed_datetime(job_df["job_listed"]))
    job_df = job_df.withColumn("job_address", extract_job_address(job_df["job_address"]))
    job_df = job_df.withColumn("job_deadline", extract_job_deadline_date(job_df["job_deadline"]))
    job_df = job_df.withColumn("employment_type", normalize_employment_type(job_df["employment_type"]))
    job_df = job_df.withColumn("job_level", normalize_job_function(job_df["job_function"])).drop("job_function")
    job_df = job_df.withColumn('industry', normalize_industries(job_df["Industries"])).drop("Industries")
    job_df = extract_min_max_yoe(job_df, "job_experience_requied", "job_yoe_min", "job_yoe_max")
    job_df = extract_min_max_salary(job_df, "salary", "salary_min", "salary_max")
    job_df = job_df.withColumn("ingested_at", F.current_date())
    job_df = job_df.withColumn("updated_at", F.current_date())

    query = job_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: batch_df.write.format("mongo").mode("append").save()) \
        .start()

    # Wait for the streaming to finish
    query.awaitTermination()


if __name__ == "__main__":
    transform_and_ingest()
