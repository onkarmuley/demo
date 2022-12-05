from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\\bigdata\\dataset\\us-500.csv"

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df = df.withColumnRenamed("zip","sal").withColumnRenamed("state","dept")

win = Window.partitionBy(col("dept")).orderBy(col("sal").desc())

df = df.select("first_name","dept","sal")
df = df.withColumn("rnk", rank().over(win))\
    .withColumn("d_rnk",dense_rank().over(win))\
    .withColumn("r_no", row_number().over(win))\
    .withColumn("n-tile", ntile(4).over(win))\
    .withColumn("percentile-rank", percent_rank().over(win))

df.show(40)
