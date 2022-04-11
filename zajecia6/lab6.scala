// Databricks notebook source
// MAGIC %sql
// MAGIC Create database if not exists Sample

// COMMAND ----------

spark.sql("Create database if not exists Sample")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Transactions ( AccountId INT, TranDate DATE, TranAmt DECIMAL(8, 2));
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT);

// COMMAND ----------

val columns_transactions = Seq("AccountId","TranDate", "TranAmt")

val columns_logical = Seq("RowID","FName", "Salary")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC INSERT INTO Sample.Transactions VALUES 
// MAGIC ( 1, '2011-01-01', 500),
// MAGIC ( 1, '2011-01-15', 50),
// MAGIC ( 1, '2011-01-22', 250),
// MAGIC ( 1, '2011-01-24', 75),
// MAGIC ( 1, '2011-01-26', 125),
// MAGIC ( 1, '2011-01-28', 175),
// MAGIC ( 2, '2011-01-01', 500),
// MAGIC ( 2, '2011-01-15', 50),
// MAGIC ( 2, '2011-01-22', 25),
// MAGIC ( 2, '2011-01-23', 125),
// MAGIC ( 2, '2011-01-26', 200),
// MAGIC ( 2, '2011-01-29', 250),
// MAGIC ( 3, '2011-01-01', 500),
// MAGIC ( 3, '2011-01-15', 50 ),
// MAGIC ( 3, '2011-01-22', 5000),
// MAGIC ( 3, '2011-01-25', 550),
// MAGIC ( 3, '2011-01-27', 95 ),
// MAGIC ( 3, '2011-01-30', 2500)

// COMMAND ----------

val transactions = Seq(( 1, "2011-01-01", 500),
( 1, "2011-01-15", 50),
( 1, "2011-01-22", 250),
( 1, "2011-01-24", 75),
( 1, "2011-01-26", 125),
( 1, "2011-01-28", 175),
( 2, "2011-01-01", 500),
( 2, "2011-01-15", 50),
( 2, "2011-01-22", 25),
( 2, "2011-01-23", 125),
( 2, "2011-01-26", 200),
( 2, "2011-01-29", 250),
( 3, "2011-01-01", 500),
( 3, "2011-01-15", 50 ),
( 3, "2011-01-22", 5000),
( 3, "2011-01-25", 550),
( 3, "2011-01-27", 95 ),
( 3, "2011-01-30", 2500)).toDF("AccountId", "TranDate", "TranAmt")

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Sample.Logical
// MAGIC VALUES (1,'George', 800),
// MAGIC (2,'Sam', 950),
// MAGIC (3,'Diane', 1100),
// MAGIC (4,'Nicholas', 1250),
// MAGIC (5,'Samuel', 1250),
// MAGIC (6,'Patricia', 1300),
// MAGIC (7,'Brian', 1500),
// MAGIC (8,'Thomas', 1600),
// MAGIC (9,'Fran', 2450),
// MAGIC (10,'Debbie', 2850),
// MAGIC (11,'Mark', 2975),
// MAGIC (12,'James', 3000),
// MAGIC (13,'Cynthia', 3000),
// MAGIC (14,'Christopher', 5000);

// COMMAND ----------

val logical = Seq((1,"George", 800),
(2,"Sam", 950),
(3,"Diane", 1100),
(4,"Nicholas", 1250),
(5,"Samuel", 1250),
(6,"Patricia", 1300),
(7,"Brian", 1500),
(8,"Thomas", 1600),
(9,"Fran", 2450),
(10,"Debbie", 2850),
(11,"Mark", 2975),
(12,"James", 3000),
(13,"Cynthia", 3000),
(14,"Christopher", 5000)).toDF("RowID" ,"FName" , "Salary" )

// COMMAND ----------

// MAGIC %md 
// MAGIC Totals based on previous row

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- running total of all transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTotalAmt
// MAGIC FROM Sample.Transactions ORDER BY AccountId, TranDate;

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val wind = Window.partitionBy("AccountId").orderBy("TranDate")
val RunTotalTransactions = transactions.withColumn("RunTotalAmt",sum("TranAmt").over(wind)).orderBy("AccountId", "TranDate")
display(RunTotalTransactions)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- running average of all transactions
// MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunAvg,
// MAGIC -- running total # of transactions
// MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTranQty,
// MAGIC -- smallest of the transactions so far
// MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunSmallAmt,
// MAGIC -- largest of the transactions so far
// MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunLargeAmt,
// MAGIC -- running total of all transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) RunTotalAmt
// MAGIC FROM Sample.Transactions 
// MAGIC ORDER BY AccountId,TranDate;

// COMMAND ----------

val wind = Window.partitionBy("AccountId").orderBy("TranDate")
val df = transactions
  .withColumn("RunAvg",avg("TranAmt").over(wind))
  .withColumn("RunTranQty",count("*").over(wind))
  .withColumn("RunSmallAmt",min("TranAmt").over(wind))
  .withColumn("RunLargeAmt",max("TranAmt").over(wind))
  .withColumn("RunTotalAmt",sum("TranAmt").over(wind))
  .orderBy("AccountId", "TranDate")
display(df)

// COMMAND ----------

// MAGIC %md 
// MAGIC * Calculating Totals Based Upon a Subset of Rows

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- average of the current and previous 2 transactions
// MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideAvg,
// MAGIC -- total # of the current and previous 2 transactions
// MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideQty,
// MAGIC -- smallest of the current and previous 2 transactions
// MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMin,
// MAGIC -- largest of the current and previous 2 transactions
// MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMax,
// MAGIC -- total of the current and previous 2 transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideTotal,
// MAGIC ROW_NUMBER() OVER (PARTITION BY AccountId ORDER BY TranDate) AS RN
// MAGIC FROM Sample.Transactions 
// MAGIC ORDER BY AccountId, TranDate, RN

// COMMAND ----------

val wind = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2,Window.currentRow)
val wind2 = Window.partitionBy("AccountId").orderBy("TranDate")
val df = transactions
  .withColumn("SlideAvg",avg("TranAmt").over(wind))
  .withColumn("SlideQty",count("*").over(wind))
  .withColumn("SlideMin",min("TranAmt").over(wind))
  .withColumn("SlideMax",max("TranAmt").over(wind))
  .withColumn("SlideTotal",sum("TranAmt").over(wind))
  .withColumn("RN", row_number().over(wind2))
  .orderBy("AccountId", "TranDate")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC * Logical Window

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT RowID,
// MAGIC FName,
// MAGIC Salary,
// MAGIC SUM(Salary) OVER (ORDER BY Salary ROWS UNBOUNDED PRECEDING) as SumByRows,
// MAGIC SUM(Salary) OVER (ORDER BY Salary RANGE UNBOUNDED PRECEDING) as SumByRange,
// MAGIC 
// MAGIC FROM Sample.Logical
// MAGIC ORDER BY RowID;

// COMMAND ----------

val wind = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val wind2 = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)
val df = logical
  .withColumn("SumByRows",sum("Salary").over(wind))
  .withColumn("SumByRange",sum("Salary").over(wind2))
  .orderBy("RowID")
display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC ROW_NUMBER() OVER (PARTITION BY TranAmt ORDER BY TranDate) AS RN
// MAGIC FROM Sample.Transactions
// MAGIC ORDER BY TranDate
// MAGIC LIMIT 10; 

// COMMAND ----------

val wind = Window.partitionBy("TranAmt").orderBy("TranDate")
val df = transactions
  .withColumn("RN",row_number().over(wind))
  .orderBy("TranDate")
  .head(10)
display(df)

// COMMAND ----------

val wind  = Window.partitionBy($"AccountId").orderBy($"TranDate")
val wind2=wind.rowsBetween(Window.unboundedPreceding, -1) 

transactions
  .withColumn("RunLead",lead("TranAmt",1) over wind )
  .withColumn("RunLag",lag("TranAmt",1) over wind)
  .withColumn("RunFirstValue",first("TranAmt") over wind2)
  .withColumn("RunLastValue",last("TranAmt") over wind2)
  .withColumn("RunRowNumber",row_number() over wind )
  .withColumn("RunDensRank",dense_rank() over wind)
  .orderBy("AccountId","TranDate")
  .show()

// COMMAND ----------

val wind=Window.partitionBy(col("AccountId")).orderBy("TranAmt")
val wind2=wind.rangeBetween(-5 ,Window.currentRow)

transactions
.withColumn("RunFirstValue",first("TranAmt").over(wind2))
.withColumn("RunLastValue",last("TranAmt").over(wind2))
.orderBy("AccountId", "TranAmt").show()

// COMMAND ----------

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val df = spark.read
      .format("jdbc")
      .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
      .option("user","sqladmin")
      .option("password","$3bFHs56&o123$")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("query",s"SELECT * FROM SalesLT.ProductCategory")
      .load()

val df2 = spark.read
      .format("jdbc")
      .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
      .option("user","sqladmin")
      .option("password","$3bFHs56&o123$")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("query",s"SELECT * FROM SalesLT.vGetAllCategories")
      .load()

// COMMAND ----------

//display(df)
display(df2)

// COMMAND ----------

val LeftSemiJoin = df.join(df2, df("ProductCategoryID") === df2("ProductCategoryID"), "leftsemi")
LeftSemiJoin.explain()

// COMMAND ----------

val LeftAntiJoin = df.join(df2, df("ProductCategoryID") === df2("ProductCategoryID"), "leftanti")
LeftAntiJoin.explain()

// COMMAND ----------

val res = df.join(df2, df("ProductCategoryID") === df2("ProductCategoryID")).drop(df("ProductCategoryID"))
display(res)

// COMMAND ----------

val res = df.join(df2,Seq("ProductCategoryID"))
display(res)

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast
val res = df.join(broadcast(df2), df("ProductCategoryID") === df2("ProductCategoryID"))

res.explain()
