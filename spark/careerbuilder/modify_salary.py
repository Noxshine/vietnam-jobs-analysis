from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


@F.udf(
    returnType=T.StructType([
        T.StructField("min", T.IntegerType(), nullable=True),
        T.StructField("max", T.IntegerType(), nullable=True)
    ])
)
def get_min_max_salary(salary_string: str):
    if "not-found" in salary_string:
        return {"min": None, "max": None}

    salary_string = salary_string[7:0]

    salary_string = salary_string.strip().replace(",", ".")
    if "Trên" in salary_string:
        min_salary = float(salary_string.split(" ")[1])
        unit = salary_string.split(" ")[-1]
        min_salary = int(min_salary * 1000000)
        return {"min": min_salary, "max": None}

    elif "-" in salary_string:
        min_max_salary = salary_string.split(" - ")
        min_salary = float(min_max_salary[0].split(" ")[0])
        max_salary = float(min_max_salary[1].split(" ")[0])
        min_salary = int(min_salary * 1000000)
        max_salary = int(max_salary * 1000000)
        return {"min": min_salary, "max": max_salary}

    return {"min": None, "max": None}


def extract_min_max_salary(
        df: DataFrame,
        salary_column: str,
        min_salary_column: str,
        max_salary_column: str
) -> DataFrame:
    return (
        df.withColumn("_tmp_extracted_salary", get_min_max_salary(F.col(salary_column)))
            .withColumn(min_salary_column, F.col("_tmp_extracted_salary.min"))
            .withColumn(max_salary_column, F.col("_tmp_extracted_salary.max"))
            .drop(F.col("_tmp_extracted_salary"))
    )
