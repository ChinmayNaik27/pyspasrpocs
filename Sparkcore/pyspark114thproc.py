#Cleanzing data obtained from kafka Streaming
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.readStream.format("kafka").option('kafka.bootstrap.servers',"localhost:9092").option("subscribe","indiapak").load()
ndf=df.selectExpr("CAST (key AS STRING)","CAST (value AS STRING)")
ex=r'(\S+),(\S+),(\S+)'
qry1=ndf.select(regexp_extract('value',ex,1).alias("name"),regexp_extract('value',ex,2).alias('age'),regexp_extract('value',ex,3).alias("city"))
qry=qry1.writeStream.format("console").start()
qry.awaitTermination()