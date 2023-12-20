import pyspark.sql.functions as F
import pyspark.sql.types as T
import re


@F.udf(returnType=T.StringType())
def modify_company_name(input: str):
    input = re.sub(r'\[.*?\]', '', input)
    input = re.sub(r'\(.*?\)', '', input)
    input = input.title()

    input = input.replace("Công Ty", "")
    input = input.replace("Tnhh", "")
    input = input.replace("Cổ Phần", "")
    input = input.strip()

    return input


