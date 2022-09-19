#getting maldata from file in a different column
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:/Users/chinm/OneDrive/Desktop/new 3.txt"
#creating Schema
sch=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data).schema
#adding new column into the schema
newsch=sch.add(StructField("wrong_records",StringType(),True))
#creating a dataframe adding the schema into dataframe
df=spark.read.format("csv").schema(newsch).option("header","true").option("inferSchema","true").option("mode","PERMISSIVE")\
    .option("columnNameOfCorruptRecord","wrong_records").load(data)
df.show()
#to filter out records we need to cache records
df.cache()
df.count()
#TO filter out wrong_records records
ndf=df.select(col("wrong_records")).filter(col("wrong_records").isNotNull())
#display records
ndf.show()