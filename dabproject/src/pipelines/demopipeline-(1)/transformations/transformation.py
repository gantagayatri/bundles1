import dlt
from pyspark.sql.functions import *

@dlt.table(name="bronze_employee")
def bronze():
    return spark.read.table("assertbundles.bronze.raw_employee_data")
@dlt.table(name="silver_employee")
def silver():
    df = dlt.read("bronze_employee")
    
    return df.filter(col("age").rlike("^[0-9]+$")) \
             .withColumn("age", col("age").cast("int")) \
             .withColumn("name", trim(col("name"))) \
             .withColumn("department", trim(col("department"))) \
             .withColumn("email", lower(trim(col("email")))) \
             .filter(col("email").contains("@"))
@dlt.table(name="gold_employee")
def gold():
    df = dlt.read("silver_employee")
    return df.select("id", "name", "age", "department", "email", "salary")
