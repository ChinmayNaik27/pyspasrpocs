#kafka spark integration Structured Streaming
#reading data from kafka stream  into console
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
#connecting to kafka
df=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","indiapak").load()
#printing it in terminal
qry=df.writeStream.format("console").start()
qry.awaitTermination()