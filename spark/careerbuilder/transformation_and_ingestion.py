from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.careerbuilder.extract_min_max_yoe import extract_min_max_yoe
from spark.careerbuilder.modify_company_name import modify_company_name
from spark.careerbuilder.modify_employment_type import normalize_employment_type
from spark.careerbuilder.modify_job_deadline import convert_to_job_deadline_datetime
from spark.careerbuilder.modify_job_listed import convert_to_job_listed_datetime
from spark.careerbuilder.modify_job_title import modify_job_title
from spark.careerbuilder.modify_salary import extract_min_max_salary
from spark.careerbuilder.normalize_industries import normalize_industries
from spark.careerlink.normalize_job_function import normalize_job_function

kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "careerbuilder"


def transform_and_ingest():
    spark = SparkSession.builder \
        .appName("careerbuilder") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/hoangph34.careerbuilder") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/hoangph34.careerbuilder") \
        .getOrCreate()

    job_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .load()

    job_df = job_df.withColumn("company_name", modify_company_name(job_df["company_name"]))
    job_df = job_df.withColumn("job_title", modify_job_title(job_df["job_title"]))
    job_df = job_df.withColumn("job_listed", convert_to_job_listed_datetime(job_df["job_listed"]))
    job_df = job_df.withColumn("job_deadline", convert_to_job_deadline_datetime(job_df["job_deadline"]))
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


    # if DeltaTable.isDeltaTable(spark, ingest_table):
    #     deltaTable = DeltaTable.forPath(spark, ingest_table)
    #     deltaTable, job_df = merge_schema(spark, deltaTable, job_df)
    #     delta_df = deltaTable.toDF()
    #     cols = {}
    #     for col in job_df.columns:
    #         if col == "ingested_at":
    #             continue
    #         cols[col] = F.when(job_df[col].isNotNull(), job_df[col]).otherwise(delta_df[col])
    #     deltaTable.alias("ingestion_table").merge(
    #         job_df.alias("daily_table"),
    #         "ingestion_table.job_id = daily_table.job_id"
    #     ).whenMatchedUpdate(set=cols).whenNotMatchedInsertAll()
    #
    # else:
    #     job_df.write.format("delta").save(ingest_table)


if __name__ == "__main__":
    transform_and_ingest()
