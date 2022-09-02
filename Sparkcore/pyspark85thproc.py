#processing simple xml data
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\DATASETS\\books.xml"
df=spark.read.format('xml').option('rowTag','book').load(data)
df.show()