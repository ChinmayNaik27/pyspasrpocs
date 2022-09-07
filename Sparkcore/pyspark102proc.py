#reading data from hbase nosql db
from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.master('local[2]').appName('test1').getOrCreate()

df=spark.read.format('org.apache.phoenix.spark').option('table','EMP').option('zkUrl','localhost:2181').load()

df.show()