#cleanzing data from complex nested json dataset
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\DATASETS\\world_bank.json"
odata="E:\\outputofjson\\complexjson"
df=spark.read.format('json').option('multiLine','true').load(data)
df.show()
df.printSchema()
#expoding contents of array
res=df.withColumn("majorsector_percent",explode(col("majorsector_percent"))).withColumn("mjsector_namecode",explode(col('mjsector_namecode'))).\
    withColumn("mjtheme",explode(col("mjtheme"))).withColumn("mjtheme_namecode",explode(col('mjtheme_namecode')))\
    .withColumn("projectdocs",explode(col("projectdocs"))).withColumn("sector",explode(col('sector')))\
    .withColumn("sector_namecode",explode(col('sector_namecode'))).withColumn("theme_namecode",explode(col('theme_namecode')))
res.show()
res.printSchema()
#extracting data from struct datatype
res1=res.withColumn("idoid",col('_id.$oid')).drop('_id').withColumn("majorsector_percent_name",col("majorsector_percent.Name"))\
    .withColumn("majorsector_percent_percent",col("majorsector_percent.Percent")).withColumn("mjsector_namecode_code",col("mjsector_namecode.code"))\
    .withColumn("mjsector_namecode_name",col("mjsector_namecode.name")).withColumn("mjtheme_namecode_code",col("mjtheme_namecode.code"))\
    .withColumn("mjtheme_namecode_name",col('mjtheme_namecode.name')).withColumn("project_abstract",col("project_abstract.cdata"))\
    .withColumn("projectdocs_docdate",col("projectdocs.DocDate")).withColumn("projectdocs_doctype",col("projectdocs.DocType"))\
    .withColumn("projectdocs_doctype_desc",col("projectdocs.DocTypeDesc")).withColumn("projectdocs_docurl",col("projectdocs.DocURL"))\
    .withColumn("projectdocs_EntityID",col("projectdocs.ENtityID")).withColumn("sector_name",col('sector.Name')).withColumn("sector1_name",col('sector1.Name'))\
    .withColumn("sector1_percent",col('sector1.Percent')).withColumn("sector2_name",col('sector2.Name'))\
    .withColumn("sector2_percent",col('sector2.Percent')).withColumn("sector3_name",col('sector3.Name'))\
    .withColumn("sector3_percent",col('sector3.Percent')).withColumn("sector4_name",col('sector4.Name'))\
    .withColumn("sector4_percent",col('sector4.Percent')).withColumn("sector_namecode_code",col('sector_namecode.code'))\
    .withColumn("sector_namecode_name",col('sector_namecode.name')).withColumn("theme1_Name",col('theme1.Name'))\
    .withColumn("theme1_Percent",col('theme1.Percent')).withColumn("theme_namecode_code",col('theme_namecode.code'))\
    .withColumn("theme_namecode_name",col('theme_namecode.name'))\
    .drop('majorsector_percent','mjsector_namecode','mjtheme_namecode','projectdocs','sector','sector1','sector2','sector3','sector4','sector_namecode','theme_namecode','theme1')
res1.show(truncate=False)
res1.printSchema()
res1.write.format('csv').option('header','true').save(odata)
print("Success!! Check :",odata,"loc")