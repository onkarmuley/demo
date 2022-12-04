from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test'").getOrCreate()

data = "D:\\bigdata\\dataset\\word_count.txt"

df = spark.read.text(data)
df = df.withColumn("value", split(col("value"), " "))
df = df.withColumn("len", size(col("value")))
df = df.agg(sum(col("len")).alias("total"))
#df.show()
count = (df.collect()[0][0])
print("Total no of words {0}".format(count))

