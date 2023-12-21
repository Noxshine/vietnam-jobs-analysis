import pyspark.sql.functions as F
import pyspark.sql.types as T
import re


@F.udf(returnType=T.StringType())
def modify_company_name(x: str):
    x = re.sub(r'\[.*?\]', '', x)
    x = re.sub(r'\(.*?\)', '', x)
    x = x.title()

    x = x.replace("Công Ty", "Cty")
    x = x.replace("Tnhh", "TNHH")
    x = x.replace("Cổ Phần", "CP")
    x = x.replace("Cp", "CP")

    x = x.strip()

    return x
