from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("df_test").getOrCreate()

employe_df = spark.read.option("header", True).option("inferSchema", True).csv("./resources/empl_test_df.csv")
dep_df = spark.read.option("header", True).option("inferSchema", True).csv("./resources/dep_test_df.csv")

print("Employees whose have age less than 40 years")
employe_df.select("*").where("age < 40").show()
employe_df.filter(employe_df.age < 40).show()
print("Employees whose have age higher than 40 years")
employe_df.select("*").where("age > 40").show()
employe_df.filter(employe_df.age > 40).show()
print("Employees which joined in 2010 and have less than 40 years old")
employe_df.where("year_joined = 2010 and age < 40").show()
print("Employees which earn less than 2000 or joined before of 2010")
employe_df.where("salary < 2000 or year_joined < 2010").show()
print("Select distinct years of joined")
employe_df.select("year_joined").distinct().show()
print("Filter column which is null")
employe_df.where(col("gender").isNull()).show()
employe_df.filter("gender is null").show()
