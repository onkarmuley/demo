from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\\bigdata\\dataset\\flights.csv"

df = spark.read.csv(data, inferSchema=True, header=True)
df.show()

# Want to see each person travelled in no of cities
res = df.withColumn("dest", split(col("dest"), "-")) #split des into list
# this list contains duplicates.
res = res.withColumn("dest", array_distinct(col("dest")))
# explode list
res = res.withColumn("dest", explode(col("dest")))
# group by to get count
res = res.groupby(col("name")).agg(count("*").alias("cnt"))
res.show()