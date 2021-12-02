name := "lectures"

version := "0.1"

scalaVersion := "2.13.7"
//scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
//  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",
//  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",

  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)

