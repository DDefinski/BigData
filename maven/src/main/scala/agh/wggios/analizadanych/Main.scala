package agh.wggios.analizadanych
import agh.wggios.analizadanych.session._
import agh.wggios.analizadanych.datareader.DataReader
import agh.wggios.analizadanych.datawriter.DataWriter
import agh.wggios.analizadanych.transformation.transformation
import org.apache.log4j.Logger


object Main extends sparksession{

  def main(args: Array[String]): Unit = {
    lazy val logger:Logger=Logger.getLogger(getClass.getName)
    logger.info("start")
    val df = new DataReader("data/actors.csv").read()
    df.show()
    val filtred_df = df.filter(row => new transformation().Actor(row))
    filtred_df.show()
    new DataWriter("filtred_actors", df).write()
  }
}
