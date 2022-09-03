# Wroking with complex xml files
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
def read_xml_nested_file(df):
    cols=[]
    for col_name in df.schema.names:
        if isinstance(df.schema[col_name].dataType,ArrayType):
            df=df.withColumn(col_name,explode(col_name))
            cols.append(col_name)
        elif isinstance(df.schema[col_name].dataType,StructType):
            for field in df.schema[col_name].dataType.fields:
                print(field)
                cols.append(col(col_name+"."+field.name).alias(col_name+"_"+field.name))
        else:
            cols.append(col_name)
    df.select(cols)
    return df
def flatten(df):
    read_nested_xml_flag=True
    while read_nested_xml_flag:
        df=read_xml_nested_file(df)
        read_nested_xml_flag=False
        for col_name in df.schema.names:
            if isinstance(df[col_name].dataType,ArrayType):
                read_nested_xml_flag=True
            elif isinstance(df[col_name].dataType,StructType):
                read_nested_xml_flag=True
    return df
#creating df
data="C:\\bigdata\\DATASETS\\complexxmldata.xml"
df=spark.read.format('xml').option('rowTag','catalog_item').load(data)
ndf=flatten(df)
ndf.show()
print("Execution Success!!")