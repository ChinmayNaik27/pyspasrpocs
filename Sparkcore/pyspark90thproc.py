# Wroking with complex xml files
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
def read_json_file(df):
    cols=[]
    for col_name in df.schema.name:
        cols.append(column_name,)
