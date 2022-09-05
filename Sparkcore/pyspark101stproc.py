#using joins on a common column of daatframes
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('org.apache.spark.sql.cassandra').option('keyspace','cassdb').option('table','asl').load()
df.show()
df1=spark.read.format("org.apache.spark.sql.cassandra").option('keyspace','cassdb').option('table','emp').load()
df1.show()

#all spark joins on a common column
#inner-join
res=df.join(df1,"name").na.fill(0)
res.show()
#alternate way for inner join
res=df.join(df1,"name","inner").na.fill(0)
res.show()
#left-outer join
res=df.join(df1,"name","leftouter").na.fill(0)
res.show()
#right-outer join
res=df.join(df1,"name","rightouter").na.fill(0)
res.show()
#full outer-join
res=df.join(df1,"name","fullouter").na.fill(0)
res.show()
