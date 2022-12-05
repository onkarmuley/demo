from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test'").getOrCreate()

def word_count(str):
    str = str.split(" ")
    return len(str)

udfunc = udf(lambda x: word_count(x))
#data = "dbfs:/FileStore/shared_uploads/onkarsmuley@gmail.com/word_count.txt"
data = "D:\\bigdata\\dataset\\word_count.txt"

df1 = spark.read.text(data)
df1.printSchema()
df1 = df1.withColumn("tot", udfunc(col("value")))

a = int(df1.agg(sum(col("tot")).alias("total_words")).collect()[0][0])
print("Total no of words", a)