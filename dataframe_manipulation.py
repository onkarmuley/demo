from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\\bigdata\\dataset\\us-500.csv"

df = spark.read.csv(data, inferSchema=True, header=True)
# df.show()

# by default it will show only top 20 rows and 20 characters of each field
# to view all rows we can use parameter n=no of rows we want to display
# and truncate = False to get entire data of each field

a = int(df.count()) # to get all rows we can use this. by deault df.count() will return a dataframe of 1 row and column.
# df.show(n=a, truncate=False)

# withColumn is used to do manipulations.
# If column provided already exist then it will modify its data otherwise will create new column

# default value added as new column
res = df.withColumn("lit", lit(18))
# remove hyphen from phone no
res = df.withColumn("phone1", regexp_replace(col("phone1"), "-", ""))
# or
res = df.withColumn("phone", regexp_replace(col("phone1"), "-", "")).drop("phone1").withColumnRenamed("phone","phone1")

# if state is CA then California, OH = Ohio for other states keep code as it is
res = res.withColumn("states", when(col("state") == 'CA', 'California') \
                     .when(col("state") == 'OH', 'Ohio') \
                     .otherwise(col("state")))

res = res.withColumn("username", substring_index(col("email"), "@", 1)) # this will give 1 part of substring separated by @
# or
res = res.withColumn("domain", split(col("email"), "@")[1]) #this will give us domain

# pincode must be of 5 digit. If you find any less than that, preced it with 0
res = res.withColumn("zip", lpad(col("zip"), 5, "0"))
res.show(truncate=False)

# data masking
#abcd@gmail.cm => a**d@gmail.com
df.withColumn('pos_at', instr('email', '@')) \
  .withColumn('new_col', expr("""
        CONCAT(LEFT(email,{0}), REPEAT('*', pos_at-1-2*{0}), SUBSTR(email, pos_at-{0}))
   """.format(1))).show(truncate=False)