#kafka spark integration Structured Streaming
#reading data from kafka and writing it into terminal and converting the types of keys from binary to string
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import  *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
#loading data
df=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","indiapak").load()
#converting key to string
ary1=df.selectExpr("CAST (key AS STRING)","CAST (value AS STRING)")
ary2=df.withColumn("key",col("key").cast(StringType()))
# ary=ary2.writeStream.format("console").start()        #to give ary2 as an output
ary=ary1.writeStream.format("console").start()
ary.awaitTermination()