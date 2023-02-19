import json
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

spark = SparkSession.builder.appName("darwin_api").getOrCreate()

# Making a get request
response = requests.get('http://universities.hipolabs.com/search?country=United+States')

# print response
print(response)

# print json content
#print(response.json())

schema = StructType([
    StructField("web_pages", StringType(), True),
    StructField("state-province", StringType(), True),
    StructField("alpha_two_code", StringType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("domains", StringType(), True)])

all_df = json.loads(response.content) #BW
rdd = spark.sparkContext.parallelize(all_df) #BW
#df = spark.read.json(rdd) #Read from json but the schema is corrupt
df_schema = spark.createDataFrame(rdd, schema) #Instead we created DF with own schema
#print("DF without schema")
#df.show()
#print("DF with schema")
df_schema.show()
print("DF without schema print schema")
#df.printSchema()
#print("DF with schema print schema")
df_schema.printSchema()

df_filt = df_schema.select(col("name"), col("state-province").alias("state_province"), col("alpha_two_code"), col("country"), col("domains"))
df_filt.printSchema()
df_filt.where("state_province <> 'Pennsylvania'").show()
df_filt.where("alpha_two_code <> 'null'").show()
