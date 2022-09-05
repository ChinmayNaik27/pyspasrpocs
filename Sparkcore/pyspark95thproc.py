#using spark joins.. Full outer join
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.master('local[2]').appName('test').getOrCreate()
df=spark.read.format('org.apache.spark.sql.cassandra').option('keyspace','cassdb').option('table','asl').load()
df.show()
df1=spark.read.format("org.apache.spark.sql.cassandra").option('keyspace','cassdb').option('table','emp').load()
df1.show()
#Full-outer join
res1=df.join(df1,df.name==df1.first_name,"fullouter")
res1.show()
