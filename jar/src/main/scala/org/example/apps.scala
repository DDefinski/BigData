package org.example
import org.apache.spark.sql.SparkSession

object SparkProject  {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Moja-aplikacja")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(1))

    df.show()

    val df2 = df.na.drop()

    df2.show()

    val df3 = df.drop("job")

    df3.show()
  }
}