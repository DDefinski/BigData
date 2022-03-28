// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

val SalesLT = tabela.where("TABLE_SCHEMA == 'SalesLT'")

val table=SalesLT.select("TABLE_NAME").as[String].collect.toList

for( i <- table){
  val tab = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()

  tab.write.format("delta").mode("overwrite").saveAsTable(i)
  display(tab)
}



// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col,when, count}

def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

val tabNames = SalesLT.select("TABLE_NAME").as[String].collect.toList

//count nulls columns
for( i <- tabNames ){
  val tab = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/"+i.toLowerCase())
  tab.select(countCols(tab.columns):_*).show()
}

//count nulls rows

//fill nulls
for( i <- tabNames.map(x => x.toLowerCase())){
  val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  val tabNew= tab.na.fill("0", tab.columns)
    display(tabNew)
}

//drop nulls
for( i <- tabNames.map(x => x.toLowerCase())){
  val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  tab.na.drop("any").show(false)
}

//3 funkcje agregujące
import org.apache.spark.sql.functions._

val tab = spark.read.format("delta")
.option("header", "true")
.load("dbfs:/user/hive/warehouse/salesorderheader")

val sum1 = tab.agg(sum("TaxAmt").as("TaxSum"), sum("Freight").as("FreightSum"))
display(sum1)

val average = tab.agg(avg("TaxAmt").as("TaxAvg"), avg("Freight").as("FreightAvg"))
display(average)

val std = tab.agg(stddev("TaxAmt").as("stdTaxAmt"), mean("TaxAmt").as("meanTaxAmt"))
display(std)

//3 funkcje agg()

val tab2 = spark.read.format("delta")
            .option("header","true")
            .option("inferSchema","true")
            .load(s"dbfs:/user/hive/warehouse/product")

val res = tab2.groupBy("ProductModelId", "Color", "ProductCategoryID")
                         .agg("Color" -> "count",
                              "StandardCost"->"sum",
                              "weight"->"max")
                         
    

display(res)

