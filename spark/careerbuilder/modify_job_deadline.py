from datetime import datetime
from pyspark.sql import types as T
from pyspark.sql import functions as F


@F.udf(returnType=T.DateType())
def convert_to_job_deadline_datetime(date_string: str):
    try:
        date_string = date_string[9:]
        date_obj = datetime.strptime(date_string, "%d-%m-%Y").date()
        return date_obj
        # return date_string
    except ValueError:
        return None
