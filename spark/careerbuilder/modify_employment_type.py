import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def normalize_employment_type(employment_type: str):
    if "Nhân viên" in employment_type:
        return "full-time"
    elif "Bán thời gian" in employment_type:
        return "part-time"
    elif "Thực tập" in employment_type:
        return "intern"
    elif "Thời vụ" in employment_type:
        return "season"


@F.udf(returnType=T.BooleanType())
def is_temporory_job(employment_type: str):
    if "Thời vụ" in employment_type:
        return True
    return False
