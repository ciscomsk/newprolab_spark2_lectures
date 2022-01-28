name := "lectures"

version := "0.1"

scalaVersion := "2.13.8"
//scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
//  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",

  /** !!! Под 2.13 - нет. */
//  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1",

  "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",

  "org.scalatest" %% "scalatest" % "3.2.11" % Test
)

