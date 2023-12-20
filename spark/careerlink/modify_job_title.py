import pyspark.sql.functions as F
import pyspark.sql.types as T
import re


@F.udf(returnType=T.StringType())
def modify_job_title(input: str):
    input = re.sub(r'\[.*?\]', '', input)
    input = re.sub(r'\(.*?\)', '', input)
    input = input.title()
    input = input.strip()
    return input


