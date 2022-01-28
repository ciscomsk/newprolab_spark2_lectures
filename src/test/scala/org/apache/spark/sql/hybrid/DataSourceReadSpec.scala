package org.apache.spark.sql.hybrid

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import shared.SparkCommons

/** testOnly org.apache.spark.sql.hybrid.DataSourceReadSpec */
class DataSourceReadSpec extends AnyFlatSpec with should.Matchers with SparkCommons {
  import spark.implicits._

  val schema: StructType =
    StructType(List(
      StructField("id", LongType),
      StructField("value", StringType),
      StructField("part_id", IntegerType),
    ))

  val df: DataFrame =
    spark
      .read
      .format("hybrid-csv")
      .schema(schema)
      .option("path", "src/main/resources/l_3/test-hybrid")
      .load

  val dfWithNull: DataFrame =
    spark
      .read
      .format("hybrid-csv")
      .schema(schema)
      .option("path", "src/main/resources/l_4/hybrid-null")
      .load

  "Reader" should "read" in {
//    df.printSchema
//    df.show

//    df
////      .select($"value")
////      .select($"value", $"id")
//      .select($"value", $"id", $"part_id")
////      .select($"id")
////      .select($"id", $"value", $"part_id")
////      .select($"part_id", $"id")
////      .explain(true)
//      .show()

//    df
//      .groupBy($"value")
//      .count()
//      .explain(extended = true)

//    df.explain(true)

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
   * l_3
   *
   * 1. org.apache.spark.sql.AnalysisException: org.apache.spark.sql.hybrid.HybridRelation is not a valid Spark SQL Data Source.
   * => 2. java.lang.AssertionError: assertion failed: No plan for Relation [id#0L,value#1L,pat_id#2] org.apache.spark.sql.hybrid.HybridRelation$$anon$2@5241f256
   */

  /**
   * l_4_1
   *
   * 1. scala.NotImplementedError
   * => 2. java.lang.ClassCastException: java.lang.Long cannot be cast to org.apache.spark.unsafe.types.UTF8String
   */

  "Column pruning" should "work 1" in {
    val filteredDf: DataFrame = df.select($"value", $"id", $"part_id")
    filteredDf.show()
  }

  "Predicate pushdown" should "work" in {
    val filteredDf: DataFrame =
      df
        //        .filter($"id" === 2)
        //        .filter($"value".contains("hello"))
        .filter($"id" > 0)
        //        .filter($"value" === "hello world")
        .filter($"value" === "hello world1")
        .select($"value", $"id", $"part_id")

//    filteredDf.explain(extended = true)
//    filteredDf.show()

    filteredDf.count() shouldBe 0
  }

  /**
   * l_4_2
   *
   * 1. java.lang.ArrayIndexOutOfBoundsException - count делает проекцию в 0 колонок.
   * Ошибка была в порядке действий - сначала проекция (с пустым массивом колонок), потом фильтрация по пустой проекции. Надо наоборот.
   */

  it should "work with nulls 1" in {
    dfWithNull.show()
    println(dfWithNull.count())
  }

  it should "work with nulls 2" in {
    val filteredDfWithNull: Dataset[Row] = dfWithNull.filter($"value".isNotNull)
    filteredDfWithNull.show()
  }

  it should "work with contains" in {
    val filteredDfWithNull: Dataset[Row] = dfWithNull.filter($"value".contains("hello"))
    filteredDfWithNull.show()
  }

  /**
   * l_4_3
   *
   * 1. java.lang.NullPointerException - т.е. contains не был null safe. Сделали null safe.
   */

  it should "work with different type in condition" in {
    val filteredDfWithNull: Dataset[Row] = dfWithNull.filter($"id" === 1) // Long == Int

    filteredDfWithNull.explain(extended = true)
    /** Analyzed Logical Plan => Filter (id#6L = cast(1 as bigint)), т.е. каталист скастил в нужный тип. */
    /*
      == Parsed Logical Plan ==
      'Filter ('id = 1)
      +- Relation [id#6L,value#7,part_id#8] org.apache.spark.sql.hybrid.HybridBaseRelation@7e18ced7

      == Analyzed Logical Plan ==
      id: bigint, value: string, part_id: int
      Filter (id#6L = cast(1 as bigint))
      +- Relation [id#6L,value#7,part_id#8] org.apache.spark.sql.hybrid.HybridBaseRelation@7e18ced7

      == Optimized Logical Plan ==
      Filter (isnotnull(id#6L) AND (id#6L = 1))
      +- Relation [id#6L,value#7,part_id#8] org.apache.spark.sql.hybrid.HybridBaseRelation@7e18ced7

      == Physical Plan ==
      *(1) Scan org.apache.spark.sql.hybrid.HybridBaseRelation@7e18ced7 [id#6L,value#7,part_id#8] PushedAggregates: [], PushedFilters: [*IsNotNull(id), *EqualTo(id,1)], PushedGroupby: [], ReadSchema: struct<id:bigint,value:string,part_id:int>
     */

    filteredDfWithNull.show()
  }
}
