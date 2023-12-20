from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


@F.udf(
    returnType=T.StructType([
        T.StructField("min", T.IntegerType(), nullable=True),
        T.StructField("max", T.IntegerType(), nullable=True),
    ])
)
def get_min_max_yoe(job_experience_required: str):
    if job_experience_required is None:
        return {"min": None, "max": None}
    if "năm kinh nghiệm" in job_experience_required:
        if "-" in job_experience_required:
            min_yoe, max_yoe = int(job_experience_required.split(" - ")[0]), int(job_experience_required.split(" - ")[1].split(" ")[0])
            return {"min": min_yoe, "max": max_yoe}
        elif "Hơn" in job_experience_required:
            return {"min": int(job_experience_required.split(" ")[1]), "max": None}
    return {"min": None, "max": None}
    
    
def extract_min_max_yoe(
    df: DataFrame,
    job_experience_reuired_column: str,
    job_yoe_min_column: str,
    job_yoe_max_column: str
) -> DataFrame:
    return (
        df.withColumn("_tmp_job_yoe_required", get_min_max_yoe(F.col(job_experience_reuired_column)))
        .withColumn(job_yoe_min_column, F.col("_tmp_job_yoe_required.min"))
        .withColumn(job_yoe_max_column, F.col("_tmp_job_yoe_required.max"))
        .drop(F.col("_tmp_job_yoe_required"))
    )
