from datetime import datetime, timedelta
import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.DateType())
def extract_job_deadline_date(input: str):
    if input == "Hôm nay":
        number_of_days = 0
    else:
        number_of_days = int(input.split(" ")[0])
    deadline_date = datetime.now() + timedelta(days=number_of_days)
    return deadline_date.date()
