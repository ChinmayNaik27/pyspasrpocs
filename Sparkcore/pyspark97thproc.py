#Using full outer join drop unecessary column and fill all integer datatypes col with 0
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('org.apache.spark.sql.cassandra').option('keyspace','cassdb').option('table','asl').load()
df.show()
df1=spark.read.format("org.apache.spark.sql.cassandra").option('keyspace','cassdb').option('table','emp').load()
df1.show()

#droping some rows

res=df.join(df1,df.name==df1.first_name,"fullouter").drop('first_name').na.fill(0)
res.show()
