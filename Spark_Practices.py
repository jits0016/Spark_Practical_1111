# Databricks notebook source
spark

# COMMAND ----------

flight_df = spark.read.format("csv")\
    .option("header","false").option("inferschema","false")\
        .option("mode","FAILFAST")\
            .load("/FileStore/tables/2010_summary.csv")
flight_df.show(5)

# COMMAND ----------

flight_header_df = spark.read.format("csv")\
    .option("header","true").option("inferschema","false")\
        .option("mode","FAILFAST")\
            .load("/FileStore/tables/2010_summary.csv")
flight_header_df.show(5)

# COMMAND ----------

flight_header_df.printSchema()

# COMMAND ----------

flight_header_schema_df = spark.read.format("csv")\
    .option("header","true").option("inferschema","true")\
        .option("mode","FAILFAST")\
            .load("/FileStore/tables/2010_summary.csv")
flight_header_schema_df.show(5)

# COMMAND ----------

flight_header_schema_df.printSchema()

# COMMAND ----------

flight_define_shema_df = spark.read.format("csv")\
    .option("header","false").option("inferschema","false")\
        .option(my_schema)\
        .option("mode","FAILFAST")\
            .load("/FileStore/tables/2010_summary.csv")
flight_define_schema_df.show(5)

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType

# COMMAND ----------

my_schema = StructType(
                       [
					    StructField("DEST_COUNTRY_NAME",StringType(),True),
						StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
						StructField("count",IntegerType(),True)
						])

# COMMAND ----------

f_df = spark.read.format("csv")\
    .option("header","false")\
    .option("skipRows",1)\
    .option("inferschema","false")\
    .schema(my_schema)\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/2010_summary.csv")

f_df.show(5)


# COMMAND ----------

emp_p_df = spark.read.format("CSV")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .load("/FileStore/emp.csv")

emp_p_df.show()

# COMMAND ----------

emp_f_df = spark.read.format("CSV")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","FAILFAST")\
    .load("/FileStore/emp_df.csv")

emp_f_df.show()

# COMMAND ----------

emp_schema = StructType([StructField("id",IntegerType(),True),
                         StructField("name",StringType(),True),
                         StructField("age",IntegerType(),True),
                         StructField("Salary",IntegerType(),True),
                         StructField("address",StringType(),True),
                         StructField("nominee",StringType(),True),
                        StructField("_corrupt_record",StringType(),True)])

# COMMAND ----------

emp_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .schema(emp_schema)\
    .load("/FileStore/emp.csv")

emp_df.show()

# COMMAND ----------

emp_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("mode","PERMISSIVE")\
    .schema(emp_schema)\
    .load("/FileStore/emp.csv")

emp_df.show(truncate = False)

# COMMAND ----------

emp_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .schema(emp_schema)\
    .option("badRecordsPath","/FileStore/tables/bad_recods")\
    .load("/FileStore/emp.csv")

emp_df.show(truncate = False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 
# MAGIC dbfs:/FileStore/tables/bad_recods/20240410T165043/bad_records/

# COMMAND ----------

bad_data_df = spark.read.format("json").load("/FileStore/tables/bad_recods/20240410T165043/bad_records/part-00000-5c197f75-4066-462f-b7f6-c8d15e3a0ed2")

bad_data_df.show(truncate = False)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/spark/pratical/

# COMMAND ----------

spark.read.format("json")\
	.option("inferSchema","true")\
	.option("mode","PERMISSIVE")\
	.load("/FileStore/spark/pratical/line_delimited_json.json")\
	.show(truncate=False)

# COMMAND ----------

spark.read.format("json")\
	.option("inferSchema","true")\
	.option("mode","PERMISSIVE")\
	.load("/FileStore/spark/pratical/single_file_json_with_extra_fields.json"\
	.show(truncate=False)

# COMMAND ----------

spark.read.format("json")\
	.option("inferSchema","true")\
	.option("mode","PERMISSIVE")\
    .option("multiline","true")\
	.load("/FileStore/spark/pratical/Multi_line_incorrect.json")\
	.show(truncate=False)

# COMMAND ----------

spark.read.format("json")\
	.option("inferSchema","true")\
	.option("mode","PERMISSIVE")\
	.load("/FileStore/spark/pratical/corrupted_json.json")\
	.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/spark/pratical/

# COMMAND ----------

spark.read.format("json")\
	.option("inferSchema","true")\
	.option("mode","PERMISSIVE")\
	.load("/FileStore/spark/pratical/file5.json")\
	.printSchema()

# COMMAND ----------

df = spark.read.parquet("/FileStore/spark/pratical/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz.parquet")

df.show()

# COMMAND ----------

# MAGIC %fs 
# MAGIC head(/FileStore/spark/pratical/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz.parquet)

# COMMAND ----------

spark.read.format("json")\
	.option("inferSchema","true")\
	.load("/FileStore/spark/pratical/emp_details.csv").show()









# COMMAND ----------

df= spark.read.format("csv")\
	.option("inferSchema","true")\
    .option("header","true")\
	.option("mode","PERMISSIVE")\
	.load("/FileStore/spark/pratical/aadharpancarddata.csv")\
	.show()

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

df.write.format("csv")\
    .option("header", "true")\
    .option("mode", "overwrite")\
    .option("path", "/FileStore/spark/practical/pan")\
    .partitionBy("Salary")\
    .save()


# COMMAND ----------

# MAGIC %fs head
# MAGIC dbfs:/FileStore/spark/practical/pan_card/Salary=10000/part-00002-tid-3923892475674107919-1c6c6ddd-4848-40ef-8cfd-74807d528299-5-1.c000.csv

# COMMAND ----------


my_data = [(1,   1),
(2,   1),
(3,   1),
(4,   2),
(5,   1),
(6,   2),
(7,   2)]


my_schema = ["id", "num"]

# COMMAND ----------

my_df = spark.createDataFrame(data=my_data,schema=my_schema)
my_df.show()

# COMMAND ----------

records_df = spark.read.format("csv")\
	.option("inferSchema","true")\
    .option("header","true")\
	.option("mode","PERMISSIVE")\
	.load("/FileStore/spark/pratical/10000Records.csv")\
	.printSchema()

# COMMAND ----------

rows = (1,"manish",26,5000)

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# COMMAND ----------

records_df.show()

# COMMAND ----------

my_df.select("id").show()

# COMMAND ----------

my_df.select(col("id")).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from datetime import datetime

# Create SparkSession
spark = SparkSession.builder \
    .appName("Create Employee DataFrame") \
    .getOrCreate()

# Define schema for the DataFrame
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("emp_dept", StringType(), True),
    StructField("emp_sal", FloatType(), True),
    StructField("emp_doj", DateType(), True),
    StructField("emp_age", IntegerType(), True)
])

# Sample data
data = [
    (1, "John Doe", "Engineering", 5000.0, datetime.strptime("2022-01-01", "%Y-%m-%d").date(), 30),
    (2, "Jane Smith", "HR", 6000.0, datetime.strptime("2022-02-01", "%Y-%m-%d").date(), 35),
    (3, "Alice Johnson", "Marketing", 5500.0, datetime.strptime("2022-03-01", "%Y-%m-%d").date(), 32),
    (4, "Bob Brown", "Engineering", 5200.0, datetime.strptime("2022-04-01", "%Y-%m-%d").date(), 28),
    (5, "Charlie Davis", "Finance", 6500.0, datetime.strptime("2022-05-01", "%Y-%m-%d").date(), 40),
    (6, "David Wilson", "Sales", 5800.0, datetime.strptime("2022-06-01", "%Y-%m-%d").date(), 37),
    (7, "Emma Lee", "Engineering", 5100.0, datetime.strptime("2022-07-01", "%Y-%m-%d").date(), 31),
    (8, "Frank Harris", "Finance", 6700.0, datetime.strptime("2022-08-01", "%Y-%m-%d").date(), 42),
    (9, "Grace Martinez", "HR", 6200.0, datetime.strptime("2022-09-01", "%Y-%m-%d").date(), 33),
    (10, "Henry Thompson", "Marketing", 5400.0, datetime.strptime("2022-10-01", "%Y-%m-%d").date(), 29)
]

# Create RDD from sample data
rdd = spark.sparkContext.parallelize(data)

# Create DataFrame
employ_df = spark.createDataFrame(rdd, schema)

# Show DataFrame
employ_df.show()


# COMMAND ----------

employ_df.select("emp_name").show()

# COMMAND ----------

employ_df.select("id +5 ").show()

# COMMAND ----------

employ_df.select(col("emp_id") +5 ).show()

# COMMAND ----------

employ_df.select(col("emp_id"),col("emp_name"),col("emp_age")).show()

# COMMAND ----------

employ_df.select("emp_id",col("emp_name"),\
    employ_df["emp_sal"],employ_df.emp_dept).show()


# COMMAND ----------

employ_df.select(expr("emp_id +5 ")).show

# COMMAND ----------

employ_df.select(expr("emp_id as employ_id"),expr("emp_name as employ_name"),expr("concat(emp_name,emp_dept)")).show()

# COMMAND ----------

# DBTITLE 1,Spark SQL 
employ_df.createOrReplaceTempView("emp_table")


# COMMAND ----------

spark.sql(""" 
          select * from emp_table
          """).show()

# COMMAND ----------

employ_df.select("*").show()

# COMMAND ----------

spark.sql(""" select emp_id,emp_age from emp_table """).show()

# COMMAND ----------

employ_df.printSchema()

# COMMAND ----------

employ_df.select(col("emp_id").alias("employee_id"),"emp_name","emp_age").show()

# COMMAND ----------

employ_df.filter(col("emp_sal")>5000).show()

# COMMAND ----------

employ_df.filter((col("emp_sal")>5000) & (col("emp_age")<32)).show()

# COMMAND ----------

employ_df.where(col("emp_sal")>5000).show()

# COMMAND ----------

employ_df.select("*",lit("kumar").alias("middle_name")).show()

# COMMAND ----------

employ_df.withColumn("sur_name",lit("singh")).show()

# COMMAND ----------

employ_df.withColumnRenamed("emp_id","employ_id").show()

# COMMAND ----------

employ_df.printSchema()

# COMMAND ----------

employ_df.withColumn("emp_id",col("emp_id").cast("string")).printSchema()

# COMMAND ----------

employ_df.withColumn("emp_id",col("emp_id").cast("string"))\
    .withColumn("emp_sal",col("emp_sal").cast("long")).printSchema()

# COMMAND ----------

employ_df.drop("emp_age",col("emp_doj")).show()

# COMMAND ----------

# DBTITLE 1,Spark SQL 
spark.sql("""select * from emp_table where emp_sal > 5000 and 
          emp_age < 32 """).show()

# COMMAND ----------

spark.sql("""select *, "kumar" as last_name from emp_table where emp_sal > 5000 and 
          emp_age < 32 """).show()

# COMMAND ----------

my_data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17)]

# COMMAND ----------

my_schema = ['id','name','sal','mangr_id']

# COMMAND ----------

manager_df = spark.createDataFrame(data=my_data,schema=my_schema)

# COMMAND ----------

data1=[(19 ,'Sohan',50000, 18),
(20 ,'Sima',75000,  17)]

schema_1 = ['id','name','sal','mangr_id']

manger_df1 = spark.createDataFrame(data=data1,schema=schema_1)

# COMMAND ----------

manager_df.show()

# COMMAND ----------

manager_df.count()

# COMMAND ----------

manger_df1.show()

# COMMAND ----------

manager_df.union(manger_df1).show()

# COMMAND ----------

manager_df.unionAll(manger_df1).show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# COMMAND ----------

duplicate_rows = [
    Row(id=20, name='Sima', sal=75000, mangr_id=17),
    Row(id=21, name='Raj', sal=60000, mangr_id=16),
    Row(id=22, name='Priya', sal=80000, mangr_id=18)
]

# COMMAND ----------

new_df = spark.createDataFrame(duplicate_rows)

# COMMAND ----------

# Convert duplicate_rows to a DataFrame


# Append new_df to manager_df
combined_df = manager_df.unionAll(new_df)

# COMMAND ----------

# Display the combined DataFrame
combined_df.show()

# Optionally remove duplicates
# combined_df = combined_df.dropDuplicates()

# Count the total number of rows
total_rows = combined_df.count()
print("Total number of rows after adding duplicates:", total_rows)

# COMMAND ----------

combined_df.unionAll(manger_df1).count()

# COMMAND ----------

combined_df.union(manger_df1).count()

# COMMAND ----------

manger_df1.createOrReplaceTempView("manger_df1_tabl")
combined_df.createOrReplaceTempView("duplicate_manger_df_tbl")

# COMMAND ----------

spark.sql(""" select * from manger_df1_tabl
          union 
          select * from duplicate_manger_df_tbl """ ).count()

# COMMAND ----------

spark.sql(""" select * from manger_df1_tabl
          union all 
          select * from duplicate_manger_df_tbl """ ).count()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]

wrong_schema = ['id','sal','mangr_id','name']

wrong_manger_df = spark.createDataFrame(data=wrong_column_data,schema=wrong_schema)

# COMMAND ----------

manger_df1.union(wrong_manger_df).show()

# COMMAND ----------

manger_df1.show()

wrong_manger_df.show()

# COMMAND ----------

manger_df1.unionByName(wrong_manger_df).show()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan',10),
(20 ,75000,  17,'Sima',20)]

wrong_schema = ['id','sal','mangr_id','name','bonus']

wrong_manger_df = spark.createDataFrame(data=wrong_column_data,schema=wrong_schema)

# COMMAND ----------

wrong_manger_df.union(manger_df1)

# COMMAND ----------

wrong_manger_df.select('id','sal','manger_id','name').union(manger_df1).show()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]

wrong_schema = ['id','sal','mangr_id','nam']

wrong_manger_df2 = spark.createDataFrame(data=wrong_column_data,schema=wrong_schema)

# COMMAND ----------

wrong_manger_df.unionByName(wrong_manger_df2).show()

# COMMAND ----------

emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]

# COMMAND ----------

emp_schema = ['id','name','age','salary','country','dept']

# COMMAND ----------

emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)

# COMMAND ----------

emp_df.show()

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# COMMAND ----------

emp_df.withColumn("adult",when(col("age")<18,"no")
                  .when(col("age")>18,"yes")
                  .otherwise("novalue")).show()
                

# COMMAND ----------

emp_df.withColumn("age",when(col("age").isNull(),lit(19))
                  .otherwise(col("age")))\
    .withColumn("adult",when(col("age")>18,"yes")
                .otherwise("no")).show()

# COMMAND ----------

emp_df.withColumn("age_wise",when((col("age")>0) &(col("age")<18),"minor")).show()

# COMMAND ----------

emp_df.withColumn("age_wise",when((col("age")>0) & (col("age")<18),"minor")
                  .when((col("age")>18) & (col("age")<30),"mid")
                  .otherwise("major")).show()

# COMMAND ----------

emp_df.createOrReplaceTempView("tab1")

# COMMAND ----------

spark.sql("""select *,
          case when age< 18 then 'minor'
          when age>18 then 'major'
          else 'novalue'
          end as age_wise
          from tab1 """).show()

# COMMAND ----------

my_data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(15 ,'Mohit',45000,  18),
(13 ,'Nidhi',60000,  17),      
(14 ,'Priya',90000,  18),  
(18 ,'Sam',65000,   17)
     ]

my_schema = ['id','name','sal','mangr_id']


# COMMAND ----------

manger_df = spark.createDataFrame(data=my_data,schema=my_schema)

# COMMAND ----------

manger_df.show()

# COMMAND ----------

manger_df.distinct().show()

# COMMAND ----------

manger_df.distinct("id","name").show()

# COMMAND ----------

manger_df.select("id","name").distinct().show()

# COMMAND ----------

drop_manger_df = manger_df.drop_duplicates(["id","name"]).show()

# COMMAND ----------

manger_df.sort(col("id")).show()

# COMMAND ----------

manger_df.sort(col("sal").desc(),col("name").desc()).show()

# COMMAND ----------

leet_code_data = [
    (1, 'Will', None),
    (2, 'Jane', None),
    (3, 'Alex', 2),
    (4, 'Bill', None),
    (5, 'Zack', 1),
    (6, 'Mark', 2)
]

schema_let = ['id','name','refernce_id']

# COMMAND ----------

leet_df = spark.createDataFrame(data=leet_code_data,schema=schema_let)

# COMMAND ----------

leet_df.show()

# COMMAND ----------

emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]

emp_schema = ['id','name','age','salary','country','dept']

emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)

# COMMAND ----------

emp_df.show()

# COMMAND ----------

emp_df.count()

# COMMAND ----------

emp_df.select(count("name")).show()

# COMMAND ----------

emp_df.select(count("id")).show()

# COMMAND ----------

emp_df.count()

# COMMAND ----------

emp_df.select(count("country")).show()

# COMMAND ----------

emp_df.select(count("*")).show()

# COMMAND ----------

emp_df.select(sum("salary"),max("salary"),min("salary")).show()

# COMMAND ----------

emp_df.select(sum("salary").alias("tot_sal"),max("salary").alias("max_sal"),min("salary").alias("min_sal")).show()

# COMMAND ----------

emp_df.select(sum("salary"),count("salary"),avg("salary").cast("int").alias("avg_sal")).show()

# COMMAND ----------


data= [(1,'manish',50000,'IT','india'),
(2,'vikash',60000,'sales','us'),
(3,'raushan',70000,'marketing','india'),
(4,'mukesh',80000,'IT','us'),
(5,'pritam',90000,'sales','india'),
(6,'nikita',45000,'marketing','us'),
(7,'ragini',55000,'marketing','india'),
(8,'rakesh',100000,'IT','us'),
(9,'aditya',65000,'IT','india'),
(10,'rahul',50000,'marketing','us')]

schema = ["id",'name','sal','dept','country']



# COMMAND ----------

emp_df = spark.createDataFrame(data=data,schema=schema)

# COMMAND ----------

emp_df.show()

# COMMAND ----------

emp_df.groupBy("dept")\
.agg(sum("sal")).show()

# COMMAND ----------

emp_df.createOrReplaceTempView("tab1")

# COMMAND ----------

spark.sql("""
          select dept,sum("sal")
          from tab1
          group by dept 
          """).show()

# COMMAND ----------

customer_data = [(1,'manish','patna',"30-05-2022"),
(2,'vikash','kolkata',"12-03-2023"),
(3,'nikita','delhi',"25-06-2023"),
(4,'rahul','ranchi',"24-03-2023"),
(5,'mahesh','jaipur',"22-03-2023"),
(6,'prantosh','kolkata',"18-10-2022"),
(7,'raman','patna',"30-12-2022"),
(8,'prakash','ranchi',"24-02-2023"),
(9,'ragini','kolkata',"03-03-2023"),
(10,'raushan','jaipur',"05-02-2023")]

customer_schema=['customer_id','customer_name','address','date_of_joining']


customer_df = spark.createDataFrame(data=customer_data,schema=customer_schema)

sales_data = [(1,22,10,"01-06-2022"),
(1,27,5,"03-02-2023"),
(2,5,3,"01-06-2023"),
(5,22,1,"22-03-2023"),
(7,22,4,"03-02-2023"),
(9,5,6,"03-03-2023"),
(2,1,12,"15-06-2023"),
(1,56,2,"25-06-2023"),
(5,12,5,"15-04-2023"),
(11,12,76,"12-03-2023")]

sales_schema=['customer_id','product_id','quantity','date_of_purchase']


sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

product_data = [(1, 'fanta',20),
(2, 'dew',22),
(5, 'sprite',40),
(7, 'redbull',100),
(12,'mazza',45),
(22,'coke',27),
(25,'limca',21),
(27,'pepsi',14),
(56,'sting',10)]

product_schema=['id','name','price']

product_df = spark.createDataFrame(data=product_data,schema=product_schema)

# COMMAND ----------

customer_df.show()
sales_df.show()
product_df.show()

# COMMAND ----------

from spark.sql.functions import * 

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner").show()

# COMMAND ----------

customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner")\
    .select(sales_df["customer_id"]).show()

# COMMAND ----------

customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner")\
    .select(sales_df["product_id"]).sort("product_id").show()

# COMMAND ----------

product_df.show()

# COMMAND ----------

customer_df.join(sales_df,(sales_df["customer_id"]==customer_df["customer_id"])& (sales_df["product_id"]==customer_df["product_id"]),"inner")\
    .select(sales_df["product_id"]).sort("product_id").show()

# COMMAND ----------

customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"left").show()

# COMMAND ----------

sales_df.join(product_df,sales_df["product_id"]==product_df["id"],"right").show()

# COMMAND ----------

product_df.join(sales_df,sales_df["product_id"]==product_df["id"],"left").show()

# COMMAND ----------

customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"outer").show()

# COMMAND ----------

customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"left_semi").show()

# COMMAND ----------

customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"left_anti").show()

# COMMAND ----------

customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"left").show()

# COMMAND ----------

customer_df.crossJoin(sales_df).count()

# COMMAND ----------

customer_df.count()

# COMMAND ----------

sales_df.count()

# COMMAND ----------

emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]

emp_schema = ['id','name','sal','dept','gender']

emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)

# COMMAND ----------

emp_df.show()

# COMMAND ----------

# DBTITLE 1,Group by 
from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.window import *

window = Window.partitionBy("dept").orderBy("sal")

emp_df.withColumn("Rank",rank().over(window))

# COMMAND ----------


product_data = [
(1,"iphone","01-01-2023",1500000),
(2,"samsung","01-01-2023",1100000),
(3,"oneplus","01-01-2023",1100000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

product_schema = ['id','product_name','sales_date','sales']

product_df = spark.createDataFrame(data=product_data,schema=product_schema)

from pyspark.sql.functions import *

emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]

emp_schema=['id','name','salary','dept','gender']

emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df = emp_df.select('id','name','salary','gender','dept')
emp_df.show()

# COMMAND ----------

product_df.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.window import *

# COMMAND ----------

window = Window.partitionBy("id").orderBy("sales_date")

last_month_df = product_df.withColumn("privous_month_sales",lag(col("sales"),1).over(window))

last_month_df.show()

# COMMAND ----------

window = Window.partitionBy("id").orderBy("sales_date")

last_month_df = product_df.withColumn("privous_month_sales",lead(col("sales"),1).over(window))

last_month_df.show()

# COMMAND ----------

last_month_df.withColumn("per_los_gain",round(((col("sales")-col('privous_month_sales'))/col("sales"))*100,2)).show()

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.window import * 
from pyspark.sql.types import * 

# COMMAND ----------

product_data = [
(2,"samsung","01-01-1995",11000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-01-2006",15000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(3,"oneplus","01-01-2010",23000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

product_schema=["product_id","product_name","sales_date","sales"]

product_df = spark.createDataFrame(data=product_data,schema=product_schema)

product_df.show()

#Q2.Data:-
emp_data = [(1,"manish","11-07-2023","10:20"),
        (1,"manish","11-07-2023","11:20"),
        (2,"rajesh","11-07-2023","11:20"),
        (1,"manish","11-07-2023","11:50"),
        (2,"rajesh","11-07-2023","13:20"),
        (1,"manish","11-07-2023","19:20"),
        (2,"rajesh","11-07-2023","17:20"),
        (1,"manish","12-07-2023","10:32"),
        (1,"manish","12-07-2023","12:20"),
        (3,"vikash","12-07-2023","09:12"),
        (1,"manish","12-07-2023","16:23"),
        (3,"vikash","12-07-2023","18:08")]

emp_schema = ["id", "name", "date", "time"]
emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)

emp_df.show()


# COMMAND ----------

window = Window.partitionBy("product_id").orderBy("sales_date").rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

# COMMAND ----------

product_df.withColumn("first_month_sales",first("sales").over(window))\
    .withColumn("latest_month_sales",last("sales").over(window)).show()

# COMMAND ----------

emp_df = emp_df.withColumn("timestamp",from_unixtime(unix_timestamp(expr("CONCAT(DATE,' ',time)"),"dd-MM-yyyy HH:mm")))

# COMMAND ----------

emp_df.show()

# COMMAND ----------

window = Window.partitionBy("id","date").orderBy("date").rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

# COMMAND ----------

new_df = emp_df.withColumn("login",first("timestamp").over(window))\
    .withColumn("logout",last("timestamp").over(window))\
        .withColumn("login",to_timestamp("logout", "yyyy-MM-dd HH:mm:ss"))\
            .withcolumn("total_time",col("logout")-col("login")).show()

# COMMAND ----------

spark

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import * 

# COMMAND ----------


resturant_json_data = spark.read.format("json")\
    .option("multiline", "true")\
    .option("inferschema", "true")\
    .load("/FileStore/spark/pratical/resturant_json_data.json")

# Print the schema of the loaded DataFrame
#resturant_json_data.printSchema()

# Explode the 'restaurants' column
exploded_df = resturant_json_data.select("*", explode("restaurants").alias("new_res"))\
    .drop("restaurants")\
.establishment_types").alias("est_type_new")).printSchema()
        .select("*","new_res.restaurants.R.res_id",explode("new_re.restaurant.establishment_types").alias("est_type_new")).printSchema()


# Print the schema of the DataFrame after exploding
exploded_df.printSchema()


# COMMAND ----------

res_json_data = spark.read.format("json")\
    .option("multiline","true")\
	.option("inferSchema","true")\
	.load("/FileStore/spark/pratical/resturant_json_data.json")\
	.printSchema()

# COMMAND ----------

res_json_data.select("*",explode("restaurants").alias("new_res")).drop("restaurants").printSchema()

# COMMAND ----------

word_json_data = spark.read.format("json")\
    .option("multiline","true")\
	.option("inferSchema","true")\
	.load("/FileStore/spark/pratical/world_bank-1.json")\
	.printSchema()

# COMMAND ----------

word_json_data.select(".",explode("id").alias("w_id")).printschema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import * 
from pyspark.sql.window import Window

# COMMAND ----------

customer_dim_data = [

(1,'manish','arwal','india','N','2022-09-15','2022-09-25'),
(2,'vikash','patna','india','Y','2023-08-12',None),
(3,'nikita','delhi','india','Y','2023-09-10',None),
(4,'rakesh','jaipur','india','Y','2023-06-10',None),
(5,'ayush','NY','USA','Y','2023-06-10',None),
(1,'manish','gurgaon','india','Y','2022-09-25',None),
]

customer_schema= ['id','name','city','country','active','effective_start_date','effective_end_date']

customer_dim_df = spark.createDataFrame(data= customer_dim_data,schema=customer_schema)

sales_data = [

(1,1,'manish','2023-01-16','gurgaon','india',380),
(77,1,'manish','2023-03-11','bangalore','india',300),
(12,3,'nikita','2023-09-20','delhi','india',127),
(54,4,'rakesh','2023-08-10','jaipur','india',321),
(65,5,'ayush','2023-09-07','mosco','russia',765),
(89,6,'rajat','2023-08-10','jaipur','india',321)
]

sales_schema = ['sales_id', 'customer_id','customer_name', 'sales_date', 'food_delivery_address','food_delivery_country', 'food_cost']

sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)


# COMMAND ----------

sales_df.show()

# COMMAND ----------

customer_dim_df.show()

# COMMAND ----------

join_data = customer_dim_df.join(sales_df,customer_dim_df["id"]==sales_df["customer_id"],"left")

# COMMAND ----------

display(join_data)

# COMMAND ----------

old_records = join_data.where(
    (col("food_delivery_address") != col("city")) & (col("active")=="Y"))\
        .withColumn("active",lit("N"))\
            .withColumn("effective_start_date",col("sales_date"))\
                .select("customer_id",
                        "customer_name",
                        "city",
                        "food_delivery_address",
                        "active",
                        "effective_start_date",
                        "effective_end_date"
)
old_records.show()

# COMMAND ----------


