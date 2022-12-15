from pyspark.sql import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

acc = spark.sparkContext.accumulator(0)

for i in range(1,11):
    acc.add(i) # or acc += i both valid

print(acc.value)