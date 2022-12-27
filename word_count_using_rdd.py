from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
data = "D:\\bigdata\\dataset\\word_count.txt"
rdd = sc.textFile(data)

rdd1 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x : (x, 1)).reduceByKey(lambda x, y: x+y)

for x in rdd1.take(rdd1.count()):
    print(x)