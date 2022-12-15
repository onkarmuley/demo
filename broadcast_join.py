from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

largeData = "D:\\bigdata\\dataset\\ratings.csv"
smallData = "D:\\bigdata\\dataset\\movies.csv"

largeDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(largeData)
smallDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(smallData)

df = largeDF.join(broadcast(smallDF),\
                  smallDF.movieId == largeDF.movieId, "inner")
df.explain(extended=False)
df.show()