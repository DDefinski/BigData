// Databricks notebook source
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}

// COMMAND ----------

val schemat = StructType(Array(
  StructField("imdb_title_id",StringType,false),
  StructField("ordering",IntegerType,false),
  StructField("imdb_name_id",StringType,false),
  StructField("category",StringType,false),
  StructField("job",StringType,false),
  StructField("characters",StringType,false)))


// COMMAND ----------

val file = spark.read.format("csv")
.option("header","true")
.schema(schemat)
.load("dbfs:/FileStore/tables/dane/actors.csv/")
display(file)

// COMMAND ----------

//val schema = new StructType().add("imdb_title_id", MapType(StringType, StringType),"ordering",MapType(StringType,IntegerType))

val schemat2 = new StructType()
  .add("imdb_title_id",StringType)
  .add("ordering",IntegerType)
  .add("imdb_name_id",StringType)
  .add("category",StringType)
  .add("job",StringType)
  .add("characters",StringType)


val file2 = spark.read.option("multiline",true)
.schema(schemat2)
.json("dbfs:/FileStore/tables/dane/csvjson.json/")
display(file2)

// COMMAND ----------

val file_badrecords = spark.read.format("csv").schema(schemat).option("badRecordsPath", "/tmp/source/badrecords").load("dbfs:/FileStore/tables/dane/actors.csv/")

val file_permissive = spark.read.format("csv").schema(schemat).option("mode","PERMISSIVE").load("dbfs:/FileStore/tables/dane/actors.csv/")

val file_dropmalformed = spark.read.format("csv").schema(schemat).option("mode","DROPMALFORMED").load("dbfs:/FileStore/tables/dane/actors.csv/")

val file_failfast = spark.read.format("csv").schema(schemat).option("mode","FAILFAST").load("dbfs:/FileStore/tables/dane/actors.csv/")

//display(file_badrecords)
//display(file_permissive)
//display(file_dropmalformed)
//display(file_failfast)

// COMMAND ----------

//file.write.parquet("dbfs:/FileStore/tables/dane/actors.parquet")
display(dbutils.fs.ls("FileStore/tables/dane/actors.parquet"))
//w docelowej lokalizacji są pliki success,committed,started i data frame podzielony na partycje

// COMMAND ----------

val paraquet_file = spark.read.parquet("dbfs:/FileStore/tables/dane/actors.parquet")
display(paraquet_file)
//wyświetla się tylko część bazowego dataframe