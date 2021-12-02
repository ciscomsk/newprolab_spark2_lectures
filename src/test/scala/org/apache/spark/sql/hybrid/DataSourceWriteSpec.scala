package org.apache.spark.sql.hybrid

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{lit, spark_partition_id}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import shared.SparkCommons

/** testOnly org.apache.spark.sql.hybrid.DataSourceWriteSpec */
class DataSourceWriteSpec extends AnyFlatSpec with should.Matchers with SparkCommons {

  val df: DataFrame =
    spark
      .range(0, 10, 1, 1)
      .withColumn("value", lit("hello world"))
      .withColumn("part_id", spark_partition_id)

  "Writer" should "write" in {
    df
      .write
      .format("hybrid-csv")
      .option("path", "src/main/resources/l_3/test-hybrid")
      .save
  }

  /**
   * 1. java.lang.ClassNotFoundException: Failed to find data source: hybrid-csv
   * => 2. java.lang.RuntimeException: org.apache.spark.sql.hybrid.HybridRelation does not allow create table as select.
   * => 3. java.io.NotSerializableException: org.apache.spark.sql.hybrid.HybridRelation
   */
}
