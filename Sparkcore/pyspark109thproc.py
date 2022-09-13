#reading spark Streaming data fetching recent data
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *

spark=SparkSession.builder.master('local[*]').appName('test').getOrCreate()
sc=spark.sparkContext
sc.setLogLevel('ERROR')
ssc=StreamingContext(sc,5)
host="ec2-13-233-158-162.ap-south-1.compute.amazonaws.com"
lines=ssc.socketTextStream(host,9999)
lines.pprint()
ssc.start()
ssc.awaitTermination()