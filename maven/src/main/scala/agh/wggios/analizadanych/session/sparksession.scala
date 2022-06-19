package agh.wggios.analizadanych.session
import org.apache.spark.sql.SparkSession

class sparksession {
  val spark: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate()
}
