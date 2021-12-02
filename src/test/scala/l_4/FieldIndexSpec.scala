package l_4

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, spark_partition_id}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import shared.SparkCommons

class FieldIndexSpec extends AnyFlatSpec with should.Matchers with SparkCommons {
  val schema: StructType =
    StructType(List(
      StructField("id", LongType),
      StructField("value", StringType),
      StructField("part_id", IntegerType)
    ))

  val writeDf: DataFrame =
    spark
      .range(0, 10, 1, 1)
      .withColumn("value", lit("hello world"))
      .withColumn("part_id", spark_partition_id)

//  writeDf
//    .write
//    .format("hybrid-csv")
//    .option("path", "src/main/resources/l_4/test-hybrid")
//    .save

  val readDf: DataFrame =
    spark
      .read
      .format("hybrid-csv")
      .schema(schema)
      .option("path", "src/main/resources/l_4/test-hybrid")
      .load()

  "Field index" should "work" in {
    val schema: StructType = readDf.schema

    schema.fieldIndex("id") shouldBe 0
    schema.fieldIndex("value") shouldBe 1
    schema.fieldIndex("part_id") shouldBe 2
  }

}
