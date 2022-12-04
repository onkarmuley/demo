from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\\bigdata\\dataset\\zips.json"
df = spark.read.format("json").option("inferSchema", "true").option("mode","DROPMALFORMED").load(data)
df = df.withColumnRenamed("_id","id")
df = df.withColumn("id", col("id").cast(IntegerType()))
df = df.withColumn("latitude", col("loc")[0]).withColumn("longitude", col("loc")[1])
df = df.drop("loc")
df.printSchema()
#df.show()

res = df.groupBy(col("state")).agg(count("*").alias("cnt"))
res.show()