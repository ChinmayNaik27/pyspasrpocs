#writing sparkdata into rds
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

from pyspark.streaming import *
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]
def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: w.split(','))
        wordsDataFrame =rowRdd.toDF(["name","age","city"])
        wordsDataFrame.show()

        # Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame1 = spark.sql("select name,count(*) as total from words group by name")
        wordCountsDataFrame1.show()
        # storing data to mysql
        host="jdbc:mysql://mysqldb1.co7gi3agncec.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
        usr="myuser"
        pwd="password"
        dvr="com.mysql.jdbc.Driver"
        tab="newtabprac"
        wordCountsDataFrame1.write.mode("append").format("jdbc").option('url',host)\
            .option('user',usr).option('password',pwd).option('dbtable',tab).option('driver',dvr).save()

    except:
        pass

sc.setLogLevel("ERROR")
ssc=StreamingContext(sc,10)
host="ec2-13-233-158-162.ap-south-1.compute.amazonaws.com"
lines=ssc.socketTextStream(host,9999)
lines.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
