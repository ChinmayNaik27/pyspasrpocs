# Wroking with complex xml files
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
def read_json_file(df):
    cols=[]
    for col_name in df.schema.name:
        if isinstance(df.schema[col_name].dataType(),ArrayType):
            df=df.withColumn(col_name,explode(col_name))
            cols.append(col_name)
        elif isinstance(df.shema[col_name].dataType(),StructType):
            for field in df.schema[col_name].dataType.fields:
                print(field)
                cols.append(col(col_name)+"."+field.name.alias(col_name+"_"+field.name))
        else:
            cols.append(col_name)
    df.select(cols)
    return df