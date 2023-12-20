from datetime import datetime
from pyspark.sql import types as T
from pyspark.sql import functions as F


@F.udf(returnType=T.DateType())
def convert_to_job_listed_datetime(date_string: str):
    date_obj = datetime.strptime(date_string, "%d-%m-%Y")
    return date_obj
