#Performing masking operation on a card number masking the values of card number(Addhar/pan)
#eg :87765492346 expected output is ********2346 for all values in a column and store it in LFS
from pyspark.sql import *
from pyspark.sql.functions import *

def mask_values(df):
    return "********"+df
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:/Users/chinm/Downloads/CC_Records.csv"
#reading data
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df.show()
df.printSchema()
#explaining udf
myfunc=udf(mask_values)
#masking values:
res=df.withColumn("CardNumber",myfunc(substring(col("CardNumber"),13,16)))
res.show()
#saveing to Lfs
res.write.format("csv").mode("append").option("header","true").option("inferSchema","True").save("C:/Users/chinm/OneDrive/Desktop")
