from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

host = "jdbc:sqlserver://sateesh.c07uo2a23um4.ap-south-1.rds.amazonaws.com:1433;databaseName=testing"

# in this we will connect to MS-SQl to extract data, transform it with dataframes and load it again to MS-SQL database
#extract data
df = spark.read.format("jdbc").option("url", host)\
    .option("user", "dbuser")\
    .option("password", "mspassword")\
    .option("dbtable", "EMP1")\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .load()
#df.show()

'''
driver name will be changed based on databases
DBMS	Driver class
SQL Server (Microsoft driver)	com.microsoft.sqlserver.jdbc.SQLServerDriver
Oracle	oracle.jdbc.OracleDriver
MariaDB	org.mariadb.jdbc.Driver
MySQL	com.mysql.jdbc.Driver

in spark config, we have to add respective jar
'''

#trasform data
res = df.na.fill(0)\
    .withColumn("full_name", concat(col("ename"), lit(" "), col("job")))\
    .withColumn("CTC", (col("sal") + col("comm"))*12)
#res.show()

#load data
res.write.format("jdbc").option("url", host)\
    .option("user", "dbuser")\
    .option("password", "mspassword")\
    .option("dbtable", "EMP1234")\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .save()

print("Data stored successfully")