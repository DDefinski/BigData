// Databricks notebook source
import scala.util.parsing.json.JSON

val input_file = "dbfs:/FileStore/tables/Nested.json"
val df = spark.read.format("json")
                         .option("inferSchema", "true")
                         .option("multiLine", "true")
                         .load(input_file)

// COMMAND ----------

display(df)

// COMMAND ----------

df.printSchema()

// COMMAND ----------

val df2 = df.withColumn("nowaCol",$"pathLinkInfo".dropFields("alternateName","captureSpecification","cycleFacility"))

// COMMAND ----------

display(df2)

// COMMAND ----------

val list = List(1, 2, 3 ,4)
val result = list.foldLeft(1)(_ * _)
println(result)

// COMMAND ----------


