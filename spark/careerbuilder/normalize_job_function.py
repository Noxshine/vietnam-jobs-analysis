import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def normalize_job_function(job_function: str):
    if "Thực tập sinh" in job_function:
        return "Thực tập sinh"
    elif "Nhân viên" in job_function:
        return "Nhân viên"
    elif "Quản lý" in job_function:
        return "Quản lý"
    elif "/" in job_function:
        return job_function.split("/")[0].strip()

    return job_function
