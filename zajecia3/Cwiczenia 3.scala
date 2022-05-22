// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._

val start = System.currentTimeMillis()

val filePath = "dbfs:/FileStore/tables/dane/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

val end = System.currentTimeMillis()
val time = (end - start)

val newnamesDF = namesDf.withColumn("create_time",lit(time))
.withColumn("feet_height",col("height")/30.48)




val mostPopularName = newnamesDF.select(split(col("name")," ")
.getItem(0).as("Name"))
.groupBy($"Name")
.count()
.orderBy(desc("count"))
.take(1)

display(mostPopularName)
//najpopularniejsze imie to John

//lata - brak

val dropped = newnamesDF.drop("bio").drop("death_details")

display(dropped)

var newnamesDF2 = namesDf
for(col <- namesDf.columns){
  newnamesDF2 = newnamesDF2.withColumnRenamed(col,col.replaceAll("_", " "))
}
newnamesDF2 = newnamesDF2.toDF(newnamesDF2.columns map(_.toUpperCase): _*)
display(newnamesDF2)

val nameOrder = namesDf.orderBy("name")

display(nameOrder)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------


val start = System.currentTimeMillis()

val filePath = "dbfs:/FileStore/tables/dane/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

val end = System.currentTimeMillis()
val time = (end - start)

var moviesDf2 = moviesDf.withColumn("time", lit(time))

display(moviesDf2)

var moviesDf3 = moviesDf2.withColumn("age", year(current_date()) - $"year")
.withColumn("budget",regexp_replace($"budget", "[^0-9]","")).na.drop()

display(moviesDf3)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val start = System.currentTimeMillis()
val filePath = "dbfs:/FileStore/tables/dane/ratings.csv"
val ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(ratingsDf)

val end = System.currentTimeMillis()
val time = (end - start)

var ratingsDf2 = ratingsDf.withColumn("time", lit(time))

display(ratingsDf2)

val ratingsDf3 = ratingsDf2.na.drop()

display(ratingsDf3)


///////////////

val ratingsDf4 = ratingsDf3.withColumn("diff_mean",  $"mean_vote" - $"weighted_average_vote" )
.withColumn("diff-median",   $"median_vote" - $"weighted_average_vote" )

display(ratingsDf4)

val males = ratingsDf4.select(avg("males_allages_avg_vote"))
val females = ratingsDf4.select(avg("females_allages_avg_vote"))

display(males)
display(females)

//kobiety oddają lepsze głosy

// COMMAND ----------

// MAGIC %md
// MAGIC zadanie 3

// COMMAND ----------

val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load("dbfs:/FileStore/tables/dane/movies.csv")

moviesDf.select($"title",$"genre").explain(true)

// COMMAND ----------

moviesDf.select($"title",$"genre").groupBy("genre").count().explain(true)

// COMMAND ----------

// MAGIC %md
// MAGIC zadanie 4

// COMMAND ----------

var username = "sqladmin"
var password = "$3bFHs56&o123$" 

val dataFromSqlServer = sqlContext.read
      .format("jdbc")
      .option("driver" , "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable", "(SELECT table_name FROM information_schema.tables) tmp")
      .option("user", username)
      .option("password",password)
      .load()

display(dataFromSqlServer)
