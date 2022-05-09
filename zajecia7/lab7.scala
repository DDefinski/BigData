// Databricks notebook source
spark.catalog.listDatabases().show()

// COMMAND ----------

spark.sql("create database db")

// COMMAND ----------

val file = spark.read.format("csv")
.option("header","true")
.option("inferSchema","true")
.load("dbfs:/FileStore/tables/dane/actors.csv/")

file.write.mode("overwrite").saveAsTable("db.actors")

// COMMAND ----------

val file = spark.read.format("csv")
.option("header","true")
.option("inferSchema","true")
.load("dbfs:/FileStore/tables/dane/names.csv/")

file.write.mode("overwrite").saveAsTable("db.names")

// COMMAND ----------

spark.catalog.listTables("db").show()


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

def fun(database: String){
  
  val tables = spark.catalog.listTables(s"$database")
  val tables_names = tables.select("name").as[String].collect.toList
  var i = List()
  for( i <- tables_names){
    spark.sql(s"DELETE FROM $database.$i")
  }
  
}

// COMMAND ----------

fun("db")
