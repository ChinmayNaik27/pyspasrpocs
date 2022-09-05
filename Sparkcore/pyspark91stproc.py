#getting data from cassendra
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
#keyspace means database of cassandera
df=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","cassdb").option('table','asl').load()
df.show()