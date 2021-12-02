import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.IntegerType

val spark: SparkSession = SparkSession
  .builder
  .master("local[*]")
  .appName("test")
  .getOrCreate

import spark.implicits._

//val fileDf =
//  spark
//    .read
//    .format("json")
//    .json("/home/mike/Downloads/1637852790.060674.jsonl")
//
//fileDf.printSchema
//fileDf.show
//
//fileDf
//  .withColumn("changestamp", $"changestamp".cast(IntegerType))
//  .show


val valid = "valid"

val df =
  spark
    .range(1, 10)
    .withColumn(valid, lit(true))
    .withColumn(valid, when($"id" > 5 , false)
      .otherwise(col(valid)))

df.show