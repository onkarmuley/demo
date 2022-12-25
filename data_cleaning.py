from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\\bigdata\dataset\\asl.csv"
df = spark.read.csv(path=data, header="true", inferSchema="true")

df.show()
df.printSchema()

#now we want upper all columns
lst = df.columns
print(lst)

res = df.select(*(upper(col(x)).alias(x) for x in lst))
res.show()