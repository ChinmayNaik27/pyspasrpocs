#reading data through spark streaming spark Streaming and reading it into rdd and dataframes
from pyspark.sql import *
from pyspark.sql.functions import *
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
        wordCountsDataFrame1 = spark.sql("select word, count(*) as total from words group by word")
        wordCountsDataFrame1.show()
    except:
        pass

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
ssc=StreamingContext(sc,5)
host="ec2-13-233-158-162.ap-south-1.compute.amazonaws.com"
lines=ssc.socketTextStream(host,9999)
lines.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
