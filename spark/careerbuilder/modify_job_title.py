import pyspark.sql.functions as F
import pyspark.sql.types as T
import re


@F.udf(returnType=T.StringType())
def modify_job_title(x: str):
    x = re.sub(r'\[.*?\]', '', x)
    x = re.sub(r'\(.*?\)', '', x)
    x = x.title()
    x = x.strip()
    return x
