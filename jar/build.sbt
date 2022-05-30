ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

sbtVersion := "1.6.2"
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "3.1.3",
  "org.apache.spark" %% "spark-sql" % "3.1.3")

lazy val root = (project in file("."))
  .settings(
    name := "app"
  )
