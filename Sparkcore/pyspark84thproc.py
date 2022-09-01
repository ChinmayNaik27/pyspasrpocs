#SHORT CODE FOR NESTED JSON (FASTER SHOTTER AND APPLICABLE FOR ANY NUMBER OF COLUMNS)
from pyspark.sql import *
from pyspark.sql.functions import *

from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\DATASETS\\world_bank.json"
odata="E:\\outputofjson\\complexjsonSHORTCODE"

def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df


def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df)
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True
    return df


def main():
    df = spark.read.option("multiline", True).option("inferSchema", False).json(data)
    df1 = flatten(df)
    # df1.coalesce(1).write.format("csv").option("header", "true").save(odata)
    #if file already exits then :
    df1.coalesce(1).write.mode('append').format("csv").option("header", "true").save(odata)
    df1.show(truncate=False)

main()
print("Successfull Execution!!")