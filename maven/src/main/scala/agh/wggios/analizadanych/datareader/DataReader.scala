package agh.wggios.analizadanych.datareader
import agh.wggios.analizadanych.session._
import agh.wggios.analizadanych.caseclass.person
import org.apache.spark.sql.{DataFrame, Dataset}

class DataReader(path: String) extends sparksession {
  def read(): Dataset[person] = {
    import spark.implicits._
    spark.read.format("csv").option("header", value = true).option("inferSchema", value = true).load(path).as[person]
  }
}
