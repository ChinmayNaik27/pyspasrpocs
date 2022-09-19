from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.master("local[*]").appName("filtermalrecords").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data="C:/Users/chinm/OneDrive/Desktop/new 3.txt"

#Creating data frame and dropping mal records
df=spark.read.format("csv").option("header","true").option("mode","DROPMALFORMED").option("inferSchema","true").load(data)
df.show()