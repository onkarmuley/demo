from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

#creating dictionary to broadcast it
states = {"NY": "New York", "CA": "California"}
broadcastedStates = spark.sparkContext.broadcast(states)

#we can broadcast many data member as we want
#num = 5
#broadcastedNum = spark.sparkContext.broadcast(num)

print(type(broadcastedStates)) #type <class 'pyspark.broadcast.Broadcast'>
#print(broadcastedStates.value) # this will give dictionary which we already created
#print(broadcastedNum.value)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

columns = ["firstname","lastname","country","state"]

df = spark.createDataFrame(data= data, schema=columns)

def get_name(code):
    if code in broadcastedStates.value.keys():
        return broadcastedStates.value[code]
    else:
        return code

my_fun = udf(get_name)
df = df.withColumn("a", my_fun(df.state))
df.show()

#we can give int, string, dictionalry, list, tuple to broadcast and whatever we provided we can access it with broadcast.value