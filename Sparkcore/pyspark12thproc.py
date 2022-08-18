#total sum ages at each location
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:/Big-Data-Files_&_notes/Spark/drivers/asl.csv"
rdd=sc.textFile(data)
res=rdd.map(lambda x:x.split(',')).filter(lambda x:"city" not in x).map(lambda x:(x[2],int(x[1]))).reduceByKey(lambda x,y:x+y)


for i in res.collect():
    print(i)