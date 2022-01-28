package org.apache.spark.sql.hybrid

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import shared.SparkCommons

/** testOnly org.apache.spark.sql.hybrid.StreamSourceSpec */
class StreamSourceSpec extends AnyFlatSpec with should.Matchers with SparkCommons {
  import spark.implicits._

  val schema: StructType = StructType(Array(
    StructField("value", IntegerType)
  ))

  /** Первый батч. */
  spark
    .range(0, 10, 1, 1)
    .select($"id".cast(IntegerType).as("value"))
    .write
    .format("hybrid-csv")
    .option("path", "src/main/resources/l_5/read-hybrid-stream")
    .save()

  "StreamReader" should "read" in {
    val streamDf: DataFrame =
      spark
        .readStream
        .schema(schema)
        .format("hybrid-csv")
        .option("path", "src/main/resources/l_5/read-hybrid-stream")
        .load()

    val sq: StreamingQuery =
      streamDf
        .writeStream
        .format("console")
        /** ProcessingTime - интервал с которым будет вызываться getOffset */
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .option("checkpointLocation", "src/main/resources/l_5/console-chk")
        .start()

    sq.awaitTermination(20000)

//    /** Второй батч. */
//    spark
//      .range(0, 10, 1, 1)
//      .select(($"id".cast(IntegerType) * -1000).as("value"))
//      .write
//      .format("hybrid-csv")
//      .option("path", "src/main/resources/l_5/read-hybrid-stream")
//      .save()

//    sq.awaitTermination(10000)

    sq.stop()
  }

  /**
   * 1. scala.NotImplementedError: an implementation is missing
   * => 2. java.lang.AssertionError: assertion failed: There are [112] sources in the checkpoint offsets and now there are [1] sources requested by the query. Cannot continue.
   *    нужно удалить директорию с чекпоинтами.
   */
}
