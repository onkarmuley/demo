from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

data = "D:\\bigdata\dataset\\asl.csv"

df = spark.read.format("csv").load(data)
# df.show()

# to consider first row as header we have to use, option("header","true")
df = spark.read.format("csv").option("header", "true").load(data)
# df.show()

df.printSchema()  # here data type of all columns are treated as string by default.
# #To get appropriate data type while reading data we have to use inferSchema property

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data)
# df.printSchema()

# handle df with sql
df.createOrReplaceTempView("temp")
res = spark.sql("select * from temp where age > 30 and city = 'mas'")
res.show()
#handling condition mentioned above in dataframe
res = df.where((col("age") > 30) & col("city").isin('mas'))
res.show()
#using not operator
res = df.where((~col("city").isin('mas')) & (col("age") > 30))
res.show()
