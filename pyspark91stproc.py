#getting data from cassendra db
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

df=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","cassdb").option('table','asl').load()
df.show()
#keyspace means database of cassendra
df1=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","cassdb").option('table','emp').load()
df1.show()

