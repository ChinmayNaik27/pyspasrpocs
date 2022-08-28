from pyspark.sql import *
from pyspark.sql.functions import *
import re
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

data="C:\\bigdata\\DATASETS\\10000Records.csv"

df=spark.read.format('csv').option('header','true').option('inferSchema','true').load(data)
# df.show()
df.printSchema()

cols=[re.sub(r'[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
ndf=df.toDF(*cols)
ndf2=ndf.withColumn("new3",to_date(col('dateofbirth'),'MM/dd/yyyy'))
ndf2.show()
ndf.printSchema()
ndf2.show()