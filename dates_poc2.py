from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\\bigdata\\dataset\\donations.csv"
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema", "true")\
    .load(data)

#Assignment = I want to get all details if trasaction happened month is ending on Sunday

df = df.withColumn("dt", to_date(col("dt"), "d-M-yyyy"))
df = df.withColumn("last_date", last_day(col("dt")))\
    .withColumn("last_day", date_format(col("last_date"), "EEE"))\
    .where(col("last_day")=="Sun")\
    .drop("last_date")\
    .drop("last_day")

df.show()
df.printSchema()