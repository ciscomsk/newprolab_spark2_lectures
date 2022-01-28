package org.apache.spark.sql.hybrid

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import shared.SparkCommons

/** testOnly org.apache.spark.sql.hybrid.StreamSinkSpec */
class StreamSinkSpec extends AnyFlatSpec with should.Matchers with SparkCommons {
  import spark.implicits._

  val streamDf: DataFrame =
    spark
      .readStream
      .format("rate")
      .load()
      .select($"value")

  "StreamWriter" should "write" in {
    /** sq - неблокирующая операция. */
    val sq: StreamingQuery =
      streamDf
      .writeStream
      .format("hybrid-csv")
      .option("path", "src/main/resources/l_5/write-hybrid-stream")
      /** !!! Чекпоинт обязателен для кастомных источников. */
      .option("checkpointLocation", "src/main/resources/l_5/write-hybrid-stream-chk")
      .start()

    /** Блокируем на 15 секунд. */
    sq.awaitTermination(15000)
    sq.stop()

    val schema: StructType = StructType(Array(
      StructField("value", IntegerType)
    ))

    val hDf: DataFrame =
      spark
        .read
        .format("hybrid-csv")
        .schema(schema)
        .option("path", "src/main/resources/l_5/write-hybrid-stream")
        .load()

    hDf.show(20, truncate = false)
  }

  /**
   * 1. java.lang.UnsupportedOperationException: Data source hybrid-csv does not support streamed writing
   * => 2. scala.NotImplementedError: an implementation is missing
   */
}
