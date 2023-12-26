from datetime import datetime
from pyspark.sql import types as T
from pyspark.sql import functions as F


@F.udf(returnType=T.DateType())
def convert_to_job_listed_datetime(date_string: str):
    try:
        date_obj = datetime.strptime(date_string, "%d-%m-%Y").date()
        return date_obj
    except ValueError:
        return None
