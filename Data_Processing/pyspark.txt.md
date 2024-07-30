
%% to install the pyspark %%

% to initialise the activity 
Gets an existing [`SparkSession`](https://spark.apache.org/docs/3.2.0/api/java/org/apache/spark/sql/SparkSession.html "class in org.apache.spark.sql") or, if there is no existing one, creates a new one based on the options set in this builder.%

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


%% to create a new dataframe %%
df = spark.createDataFrame(data, schema)

%% to show the dataframe in raw fromat %%
df.show()
df.show(n=8) %% no of rows to fetch%%
df.show(truncate = 4) %% no of columns components to fetch%%

%% to get the data type of columns %%
display(df), df

%% to read the csv file, header represents the first row as column name %%
spark.read.csv(path, header = True, * *)
spark.read.format("csv").option(key='header', value=True).load(path)
spark.read.option(key='header', value=True).csv(path, schema)

%%to read multiple files from same folders %%
spark.read.csv(path = mention folder path, header = True, *)
spark.read.parquet(path = mention folder path, header = True)
spark.read.json(path = mention folder path, header = True, multiline=True, *)

spark.read.csv(path = "complete folder path/*.csv", header = True)

%%to read multiple files from different folders %%
spark.read.csv(path = [list of file path], header = True, *)
spark.read.csv(path = [path1, path2 path3], header = True, *)



%% to write the dataframe  %%
df.write.csv(path =  file_path , header = True, mode = ["append", "error", "ignore"])
df.write.format("csv).mode("overwrite").save("path")

df.write.json(path = file_path, header = True, mode = [])
df.write.format("json").mode("overwrite").save("path")

df.write.parquet(path = file_path, header = True, mode = ["append", "error", "ignore"])
df.write.format("parquet").mode("overwrite").save("path")


%% to rename and change the data type of columns %%
from pyspark.sql.functions import col, lit
df.withColumn(colName = "name", col = col("name").cast("Integer") )

df.withColumn(colName = "name", col("name")+4)

df.withColumn(colName = "new_name", lit("value"))

df.withColumnRenamed("existing_col", "new_col")


%% Create a schema of table %%

from spark.sql.types import *
schema = StructType().add(field = "id", data_type = IntegerType())\
					.add(field="name", data_type = StringType())

%% structure the table %%
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, 
						ArrayType, MapType
structname = StructType[\
					StructField (name = "first", dataType = StringType()),
					StructField (name = "last", dataType = StringType())
					]
					
schema = StructType[\
					StructField (name = "id", dataType = IntergerType()),
					StructField (name = "candidate", dataType = structname),
					StructField (name = "city", dataType = StringType())
					StructField (name = "city", dataType = ArrayType(IntegerType(), StringType()))
					]

schema = StructType[\
					StructField (name = "id", dataType = IntergerType()),
					StructField (name = "properties", dataType = MapType(IntegerType(), StringType()))
					]

%% to visualise a proper complex columns %%
df.printSchema()


%% mutiple columns in one array %%
%% ArrayType stored in indexing format like a list %%
from pyspark.sql.functions import array, col
df.withColumn(colName ="new or existing column" , array(col("col1"), col("col2")))

%% to create all the combinations of elements in features%%
from pyspark.sql.functions import explode, col
df.withColumn(colName ="new or existing column" , explode(col("col1"))

%% to split the elements in features using delimeters%%
from pyspark.sql.functions import split, col
df.withColumn(colName ="new or existing column" , split(col("col1", "delimeter"))


%% to find the elements in features using string%%
%% it return the value in True or False%%
from pyspark.sql.functions import array_contains, col
df.withColumn(colName ="new or existing column" , array_contains(col("col1", "value"))


%% to create all the combinations of dictionary elements %%
%% other iterable objects into multiple rows%%
from pyspark.sql.functions import map_keys, map_values, explode, col
df.select("list of individual", "columns", explode(col("col1")))

%% to fetch the key from a dictionary %%
df.withColumn("key", map_keys(col("col1")))

%% to fetch the values from a dictionary %%
df.withColumn("values", map_values(col("col1")))


%% define the ROW functions%%
from pyspark.sql import Row
obj1 = Row("name", "age")
r1 = obj1("Hemant", "23")
r2 = obj1("Rahul", "28")
r3 = obj1("Nivedita", "24")
spark.createDataFrame([r1, r2, r3]).show()

spark.createDataFrame(Row({"name" : "hemant", "age" : 34})).show()

spark.createDataFrame(
[Row(name = "hemant", prop = Row(age = 34, contact_number = 7999566808)),
Row(name = "rahul", prop = Row(age = 28, contact_number = 9977646810))]
).show()

