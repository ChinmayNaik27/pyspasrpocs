#wirting data into cassandra db
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.format('org.apache.spark.sql.cassandra').option('keyspace','cassdb').option('table','asl').load()
df.show()
df1=spark.read.format("org.apache.spark.sql.cassandra").option('keyspace','cassdb').option('table','emp').load()
df1.show()

res=df.join(df1,df.name==df1.first_name,"fullouter").drop('first_name','last_name').na.fill(0).na.fill("No Data")
res.show()

#writing results into cassandra , there is already a table named newcasstable in cassdb with schema in which we have imported
res.write.format("org.apache.spark.sql.cassandra").option('keyspace','cassdb').option('table','newcasstable').save()
