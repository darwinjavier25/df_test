from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import col, desc, first

spark = SparkSession.builder.appName("df_test").getOrCreate()

employe_df = spark.read.option("header", True).option("inferSchema", True).csv("/home/dw/python/data_frames/empl_test_df.csv")
dep_df = spark.read.option("header", True).option("inferSchema", True).csv("/home/dw/python/data_frames/dep_test_df.csv")

employe_df.show()
employe_df.printSchema()
dep_df.show()
dep_df.printSchema()

#max_emp_df_sql = employe_df.select("*").where("salary = ")
print("Inner Join")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "inner").show()
print("Outer Join")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "outer").show()
print("Full Join")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "full").show()
print("FullOuter Join")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "fullouter").show()
print("Left or LeftOuter Join")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "left").show()
print("Right or RightOuter Join")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "right").show()
print("LeftSemi Join")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "leftsemi").show()
print("LeftAnti Join")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "leftanti").show()
