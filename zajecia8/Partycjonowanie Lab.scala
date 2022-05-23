// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val data = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv("dbfs:/FileStore/tables/pageviews_by_second.tsv")

data.write.mode("overwrite").parquet("dbfs:/pageviews_by_second.parquet")


// COMMAND ----------

spark.catalog.clearCache()

val df = spark.read
  .parquet("dbfs:/pageviews_by_second.parquet")
  .repartition(2000)
//  .coalesce(6)
  .groupBy("site").sum()


df.explain
df.count()

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

val df = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load("dbfs:/FileStore/tables/dane/movies.csv")

// COMMAND ----------

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._



// COMMAND ----------

display(df)

// COMMAND ----------

df.dtypes.foreach(f=>println(f._1+","+f._2))

// COMMAND ----------

//funkcja zliczająca sumę wartości w kolumnie

def SumColumn(colName: String, df: DataFrame): Any={
  if(!df.columns.contains(colName)){
      println("There is no such column")
      return 0
  }
  if(df.schema(colName).dataType.typeName == "string"){
    println("This is string column")
    return 0
  }
    return df.agg(sum(colName)).first.get(0)
  
}

// COMMAND ----------

SumColumn("duration",df)

// COMMAND ----------

//funkcja zliczająca nulle w kolumnie

def CountNullColumn(colName : String, df :DataFrame) : Long ={
  if(!df.columns.contains(colName))
  {
      println("There is no such column")
      return 1
  } 
  return df.filter(col(colName).isNull).count()
}

// COMMAND ----------

CountNullColumn("year",df)

// COMMAND ----------

//funkcja zliczająca wystąpienia poszczególnych wartości

def CountOccurances(colName : String, df :DataFrame) : DataFrame ={
  if(!df.columns.contains(colName)){
      println("There is no such column")
      return spark.emptyDataFrame
  }
  
  return df.groupBy(colName).count()
}

// COMMAND ----------

display(CountOccurances("year",df))

// COMMAND ----------

//funkcja zwracająca największą wartość liczbową z zadanej kolumny

def GetMaxColValue(colName: String, df: DataFrame): Any={
  if(!df.columns.contains(colName)){
      println("There is no such column")
      return 0
  }
  if(df.schema(colName).dataType.typeName == "string"){
    println("This is string column")
    return 0
  }
    return df.agg(max(colName)).first.get(0)
  
}

// COMMAND ----------

GetMaxColValue("duration",df)

// COMMAND ----------

//funkcja zwracająca najmniejszą wartość liczbową z zadanej kolumny

def GetMinColValue(colName: String, df: DataFrame): Any={
  if(!df.columns.contains(colName)){
      println("There is no such column")
      return 0
  }
  if(df.schema(colName).dataType.typeName == "string"){
    println("This is string column")
    return 0
  }
    return df.agg(max(colName)).first.get(0)
  
}

// COMMAND ----------

GetMinColValue("duration",df)

// COMMAND ----------

//funkcja zwracająca średnią wartość liczbową z zadanej kolumny

def CountColumnMean(colName: String, df: DataFrame): Any = {
  
  if(!df.columns.contains(colName)){
      println("There is no such column")
      return 0
  }
  if(df.schema(colName).dataType.typeName == "string"){
    println("This is string column")
    return 0
  }
      
    return df.select(avg(col(colName))).first.get(0)
      
}

// COMMAND ----------

CountColumnMean("duration",df)

// COMMAND ----------


