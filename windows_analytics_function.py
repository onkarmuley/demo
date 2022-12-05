from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\\bigdata\\dataset\\us-500.csv"

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

df = df.withColumnRenamed("zip","sal").withColumnRenamed("state","dept")
win = Window.partitionBy(col("dept")).orderBy(col("sal").desc())

df = df.select("first_name","dept","sal")
df = df.withColumn("lead", lead("sal").over(win))\
    .withColumn("lag", lag("sal").over(win))\
    .withColumn("first", first("sal").over(win))\
    .withColumn("last", last("sal").over(win))\
    .withColumn("cum-dist", cume_dist().over(win))

df.show(40)