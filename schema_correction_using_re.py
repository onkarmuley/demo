from pyspark.sql import *
from pyspark.sql.functions import *
import re

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "D:\\bigdata\dataset\\10000Records.csv"  #this is ; separated file

# to drop malformed records we can use .option("mode","DROPMALFORMED")
df = spark.read.format("csv").option("header", "true").option("sep", ";").option("inferSchema", "true").load(data)
df.show()
# column heading looks like :
# Emp ID;Name Prefix;First Name;Middle Initial;Last Name;Gender;E Mail;Father's Name;Mother's Name;
# Mother's Maiden Name;Date of Birth;Time of Birth;Age in Yrs.;Weight in Kgs.;Date of Joining;
# Quarter of Joining;Half of Joining;Year of Joining;Month of Joining;Month Name of Joining;
# Short Month;Day of Joining;DOW of Joining;Short DOW;Age in Company (Years);Salary;Last % Hike;SSN;
# Phone No. ;Place Name;County;City;State;Zip;Region;User Name;Password
# It contains spaces, special characters which are not allowed

# To correct it, we can use Regular expressions

cols = df.columns # this will all column names in list
print(cols)
cols = [re.sub("[^0-9A-Za-z]","", d) for d in cols] # here we will remove all special characters, spaces
print(cols)

df = df.toDF(*cols) # apply this column names to dataframe df
df.show()

