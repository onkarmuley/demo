
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\\bigdata\\dataset\\donations.csv"
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema", "true")\
    .load(data)

#Assignment = I want to get last Friday and 2nd Friday of the month for the date mentioned in dt column

df = df.withColumn("dt", to_date(col("dt"), "d-M-yyyy"))
df = df.withColumn("last_day", last_day(col("dt")))\
    .withColumn("-7_days", date_sub(col("last_day"), 7))\
    .withColumn("last_friday_on", next_day(col("-7_days"), "Fri"))\
    .withColumn("1st_date", date_trunc("month", col("dt")))\
    .withColumn("1st_day", date_format(col("1st_date"), "EEE"))\
    .withColumn("2nd_Fri", when(col("1st_day")=="Fri", date_add(col("1st_date"), 7))\
                .otherwise(date_add(next_day(col("1st_date"), "Fri"), 7))
                )

df.show()
df.printSchema()