package agh.wggios.analizadanych.datawriter
import agh.wggios.analizadanych.session._
import org.apache.spark.sql.{DataFrame, Dataset}
import agh.wggios.analizadanych.caseclass.person

class DataWriter(path: String, df: Dataset[person]) extends sparksession {
  def write(): Unit = {
    df.write.parquet(path)
  }
}
