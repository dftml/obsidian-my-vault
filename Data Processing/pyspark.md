
%% to install the pyspark %%

% to initialise the activity 
Gets an existing [`SparkSession`](https://spark.apache.org/docs/3.2.0/api/java/org/apache/spark/sql/SparkSession.html "class in org.apache.spark.sql") or, if there is no existing one, creates a new one based on the options set in this builder.%

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


%% to create a new dataframe %%
df = spark.createDataFrame(data, schema)

%% to show the dataframe in raw fromat %%
df.show()

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


%% Create a schema of table %%

from spark.sql.types import *
schema = StructType().add(field = "id", data_type = IntegerType())\
					.add(field="name", data_type = StringType())


%% to write the dataframe  %%
df.write.csv(path =  file_path , header = True, mode = ["append", "error", "ignore"])
df.write.format("csv).mode("overwrite").save("path")

df.write.json(path = file_path, header = True, mode = [])
df.write.format("json").mode("overwrite").save("path")

df.write.parquet(path = file_path, header = True, mode = ["append", "error", "ignore"])
df.write.format("parquet").mode("overwrite").save("path")


