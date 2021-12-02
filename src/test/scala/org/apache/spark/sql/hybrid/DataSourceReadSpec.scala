package org.apache.spark.sql.hybrid

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import shared.SparkCommons

/** testOnly org.apache.spark.sql.hybrid.DataSourceReadSpec */
class DataSourceReadSpec extends AnyFlatSpec with should.Matchers with SparkCommons {
  val schema: StructType =
    StructType(List(
      StructField("id", LongType),
      StructField("value", StringType),
      StructField("pat_id", IntegerType),
    ))

  val df: DataFrame =
    spark
      .read
      .format("hybrid-csv")
      .schema(schema)
      .option("path", "src/main/resources/l_3/test-hybrid")
      .load

  "Reader" should "read" in {
    df.printSchema
    df.show
    df.explain(true)
    /*
      == Parsed Logical Plan ==
      Relation [id#0L,value#1,pat_id#2] org.apache.spark.sql.hybrid.HybridBaseRelation@49f58079

      == Analyzed Logical Plan ==
      id: bigint, value: string, pat_id: int
      Relation [id#0L,value#1,pat_id#2] org.apache.spark.sql.hybrid.HybridBaseRelation@49f58079

      == Optimized Logical Plan ==
      Relation [id#0L,value#1,pat_id#2] org.apache.spark.sql.hybrid.HybridBaseRelation@49f58079

      == Physical Plan ==
      *(1) Scan org.apache.spark.sql.hybrid.HybridBaseRelation@49f58079 [id#0L,value#1,pat_id#2] PushedAggregates: [], PushedFilters: [], PushedGroupby: [], ReadSchema: struct<id:bigint,value:string,pat_id:int>
     */
  }

  /**
   * 1. org.apache.spark.sql.AnalysisException: org.apache.spark.sql.hybrid.HybridRelation is not a valid Spark SQL Data Source.
   * => 2. java.lang.AssertionError: assertion failed: No plan for Relation [id#0L,value#1L,pat_id#2] org.apache.spark.sql.hybrid.HybridRelation$$anon$2@5241f256
   */
}
