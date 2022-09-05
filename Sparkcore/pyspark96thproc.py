#Using full outer join drop unecessary column
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format('org.apache.spark.sql.cassandra').option('keyspace','cassdb').option('table','asl').load()
df.show()
df1=spark.read.format("org.apache.spark.sql.cassandra").option('keyspace','cassdb').option('table','emp').load()
df1.show()

#droping some rows which are not required
res=df.join(df1,df.name==df1.first_name,"fullouter").drop('first_name')
res.show()


