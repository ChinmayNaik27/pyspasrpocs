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
def flatten(df):
    read_json_flag=True
    while read_json_flag:
        df=read_json_file(df)
        read_json_flag=False
        for col_name in df.schema.name:
            if isinstance(df[col_name].dataType(),ArrayType):
                read_json_flag=True
            elif isinstance(df[col_name].dataType(),StructType):
                read_json_flag=True
#creating df
data="C:\\bigdata\\DATASETS\\books.xml"
odata="E:\\outputofxml\complexxml-output-to-csv"
df=spark.read.format('xml').option('rowType','books').load(data)
ndf=flatten(df)
print(ndf)