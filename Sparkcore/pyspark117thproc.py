#create patitions using repartition(),coalesce() and saving it to a location
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").config("spark.sql.shuffle.partitions", "500").appName("test").getOrCreate()
sc = spark.sparkContext

df = spark.range(0,20)
print(df.rdd.getNumPartitions())
#creating data in range of 0 to 20
rdd = spark.sparkContext.parallelize((0,20))
print("From local[5]"+str(rdd.getNumPartitions()))
#creating data in range of 0 to 20 with 6 partition
rdd1 = spark.sparkContext.parallelize((0,25), 6)
print("parallelize : "+str(rdd1.getNumPartitions()))

"""rddFromFile = spark.sparkContext.textFile("src/main/resources/test.txt",10)
print("TextFile : "+str(rddFromFile.getNumPartitions())) """
#saving file to given path
# rdd1.saveAsTextFile("c://tmp/partition2")

#repartition
rdd2 = rdd1.repartition(4)
print("Repartition size : "+str(rdd2.getNumPartitions()))
#saving file to given path
# rdd2.saveAsTextFile("c://tmp/re-partition2")

#coalesce
rdd3 = rdd1.coalesce(4)
print("Repartition size : "+str(rdd3.getNumPartitions()))
#saving file to given path
# rdd3.saveAsTextFile("c:/tmp/coalesce2")