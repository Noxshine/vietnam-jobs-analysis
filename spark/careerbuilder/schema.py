from pyspark.sql.types import StructType, StructField, StringType


job_schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("job_listed", StringType(), True),
    StructField("job_deadline", StringType(), True),
    StructField("salary", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("job_address", StringType(), True),
    StructField("job_experience_required", StringType(), True),
    StructField("employment_type", StringType(), True),
    StructField("job_function", StringType(), True),
    StructField("industries", StringType(), True),
    StructField("welfare", StringType(), True),
    StructField("job_description", StringType(), True),
    StructField("job_requirement", StringType(), True),

])