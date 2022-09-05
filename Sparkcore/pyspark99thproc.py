#filling null values after join
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.format('org.apache.spark.sql.cassandra').option('keyspace','cassdb').option('table','asl').load()
df.show()
df1=spark.read.format("org.apache.spark.sql.cassandra").option('keyspace','cassdb').option('table','emp').load()
df1.show()
#filing all columns which match the datatype
res=df.join(df1,df.name==df1.first_name,"fullouter").drop('first_name').na.fill(0).na.fill("No Data")
res.show()
#filling null values in selected columns
res1=df.join(df1,df.name==df1.first_name,"fullouterjoin").na.fill(0,['id']).na.fill("no data",["name","first_name"])
res1.show()
