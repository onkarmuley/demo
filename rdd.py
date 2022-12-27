from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

#lst = [1,2,3,4,5,6,7,8,9,10]
#rdd = sc.parallelize(lst)
#for x in rdd.take(rdd.count()):
#    print(x)

data = "D:\\bigdata\\dataset\\email.txt"
rdd = sc.textFile(data)

df = rdd.filter(lambda x: "@" in x).\
    map(lambda x: x.split(" ")).\
    filter(lambda x : len(x[-1])>0).\
    map(lambda x: (x[0], x[-1]))#.toDF(["name","email"])
for x in df.take(df.count()):
    print(x)
#df.show()

