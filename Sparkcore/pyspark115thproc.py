#cleanzing data obtained from kafka Stream and storing data to database
from pyspark.sql import *
from pyspark.sql.functions import *

def foreach_batch_function(df,bid):
    host="jdbc:mysql://mysqldb1.co7gi3agncec.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSl=false"
    df.write.mode("append").format("jdbc").option("url",host).option("user",'myuser')\
        .option("password",'password').option('dbtable','newtabkafka').option("driver","com.mysql.cj.jdbc.Driver").save()
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","indiapak").load()
ndf=df.selectExpr("CAST (value AS STRING)")
ex=r'^(\S+),(\S+),(\S+)'
qry1=ndf.select(regexp_extract('value',ex,1).alias("name"),regexp_extract('value',ex,2).alias("age"),regexp_extract('value',ex,3).alias("city"))
#storing data in database
qry1.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
#display database
# qry1=qry.writeStream.format("console").start()
# qry1.awaitTermination()
