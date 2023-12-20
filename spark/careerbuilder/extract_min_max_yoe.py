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
    if "Trên" in job_experience_required:
        job_experience_required = job_experience_required[6:]
        return {"min": int(job_experience_required[0]), "max": None}

    elif "Lên đến" in job_experience_required:
        job_experience_required = job_experience_required[9:]
        return {"min": None, "max": int(job_experience_required[0])}

    elif "not-found" in job_experience_required:
        return {"min": None, "max": None}

    job_experience_required = job_experience_required[:5]
    return {"min": int(job_experience_required[0]), "max": int(job_experience_required[4])}


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
