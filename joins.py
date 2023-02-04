from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import col, desc, first

spark = SparkSession.builder.appName("df_test").getOrCreate()

employe_df = spark.read.option("header", True).option("inferSchema", True).csv("./resources/empl_test_df.csv")
dep_df = spark.read.option("header", True).option("inferSchema", True).csv("./resources/dep_test_df.csv")

employe_df.show()
employe_df.printSchema()
dep_df.show()
dep_df.printSchema()

#max_emp_df_sql = employe_df.select("*").where("salary = ")
print("Inner Join: only show items join each other")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "inner").show()
print("Outer Join: show all rows which join otherwise shows null")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "outer").show()
print("Full Join: show all rows which join otherwise shows null")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "full").show()
print("FullOuter Join: show all rows which join otherwise shows null")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "fullouter").show()
print("Left or LeftOuter Join: show records which join and futhermore show records in left that not cross in right")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "left").show()
print("Right or RightOuter Join: show records which join and futhermore show records in right that not cross in left")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "right").show()
print("LeftSemi Join: show records which are in left but not in right and only show left fields")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "leftsemi").show()
print("LeftAnti Join: show records that not join each other")
employe_df.join(dep_df, employe_df.emp_dept_id == dep_df.dept_id, "leftanti").show()
