package l_2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{lit, pmod, struct, udf}
import org.apache.spark.sql.types.{IntegerType, StructType}
import shared.SparkCommons

import java.lang

object Internals_2 extends SparkCommons with App {
  import spark.implicits._

  /**
   * Работа с Row.
   *
   * org.apache.spark.sql.Row используется:
   * 1. При использовании collect.
   * 2. При применении метода rdd к DataFrame.
   * 3. При передаче колонки типа struct в UDF.
   *
   * Row представляем собой:
   * 1. Упорядоченный список Seq[Any], содержащий данные строки.
   * 2. Схему StructType, описывающую типы каждого элемента списка с данными.
   *
   * Row не используется для фактического хранения данных под капотом DataFrame, а всего лишь является удобной
   * (упорядоченный + схема) оберткой InternalRow в некоторых случаях.
   *
   * Есть энкодеры Row <=> InternalRow.
   */

  /** package org.apache.spark.sql */
  /* trait Row extends Serializable */

  val df: Dataset[lang.Long] =
  spark
    .range(0, 10)
    .localCheckpoint

  val groupedDf: DataFrame =
    df
      .filter($"id" > 0)
      .select(pmod($"id", lit(2)).as("mod2"))
      .groupBy($"mod2".cast(IntegerType).as("pmod2"))
      .count

  groupedDf.show
  groupedDf.printSchema

  val rddRow: RDD[Row] = groupedDf.rdd

  rddRow
    .take(2)
    .foreach(println)

  println

  val arr: Array[Row] = groupedDf.collect
  arr.foreach(println)
  println

  val firstRow: Row = arr.head
  println(firstRow)
  println

  /** !!! rowSchema - массив StructField - индекс имеет значение */
  val rowSchema: StructType = firstRow.schema
  rowSchema.printTreeString

  val deser: Seq[Any] = firstRow.toSeq

  deser.foreach { el =>
    println(s"$el:${el.getClass.getCanonicalName}")
  }

  /** Получение элемента по индексу - нужно приводить к типу поля, или использовать геттеры с встроенным типом. */
  val elemByIdx: Int = firstRow.getAs[Int](0) // == getInt(0)
  // ~ val elemByIdx: Integer = firstRow.getAs[java.lang.Integer](0)
  /** Получение элемента по имени поля - возможно т.к. Row энкапсулирует схему. */
  val elemByName: Int = firstRow.getAs[java.lang.Integer]("pmod2")  // == firstRow.getInt(firstRow.fieldIndex("pmod2"))
  println(elemByIdx == elemByName)
  println


  val structDf: DataFrame =
    df
      .select(
        struct($"id", lit(1).as("foo"), lit(null).cast(IntegerType).as("bar")).as("s")
      )

  structDf.printSchema

  /**
   * Row:
   * 1. тип struct - Row.
   * 2. тип array - Seq[T].
   * 3. тип map - Map[A, B].
   *
   * DataFrame:
   * 1. TimestampType - java.sql.Timestamp.
   * 2. LongType - java.lang.Long.
   * 3. ArrayType[T] - например ArrayType(IntegerType).
   * 4. StructType(
   */
  val udf_struct: UserDefinedFunction = udf { (s: Row, fieldName: String) =>
    /** !!! будет 0 - т.к. scala Int не может принимать значение null. */
//    val data: Int = s.getAs[Int](fieldName)
    /** !!! будет null - т.к. java.lang.Integer может быть null. */
    val data: java.lang.Integer = s.getAs[java.lang.Integer](fieldName)
    data
  }

  structDf
    .select(udf_struct($"s", lit("foo")))
    .show(1, truncate = false)

  structDf
    .select(udf_struct($"s", lit("bar")))
    .show(1, truncate = false)

  /** Есть энкодеры Row => Case class - но они не дают необходимой гибкости. */

  /** RDD[Row] - обертка над InternalRow. */
  val rddR: RDD[lang.Long] = df.rdd
  /** RDD[InternalRow] - данные. */
  val rddIR: RDD[InternalRow] =
    df
      .queryExecution
      .toRdd


  /**
   * Работа с InternalRow.
   *
   * org.apache.spark.sql.catalyst.InternalRow используется:
   * 1. Для фактического хранения данных в DataFrame API.
   * 2. Во время чтения источников и формирования DataFrame на их базе.
   * 3. Во время записи DataFrame в источник.
   * 4. При передаче данных между физическими операторами.
   * 5. При вызове встроенных функций над колонками.
   *
   * InternalRow:
   * 1. Содержит упорядоченный список Seq[Any] с данными.
   * 2. НЕ СОДЕРЖИТ СХЕМУ ДАННЫХ.
   * 3. on Heap реализация -  GenericInternalRow - Array[Any]
   * 3. off Heap реализация - UnsafeRow - Array[Byte], содержащий данные в сериализованном виде. Часто используется в кодогенерации.
   *
   * !!! При реализации функциональности следует использовать базовый класс InternalRow.
   * Т.к. Spark может произвольно менять виды реализаций.
   */

  /** package org.apache.spark.sql.catalyst */
  /* abstract class InternalRow extends SpecializedGetters with Serializable */

  groupedDf.show
  groupedDf.printSchema
  println(s"Partitions number: ${groupedDf.queryExecution.toRdd.getNumPartitions}")
  println

  val rddIRows: RDD[InternalRow] =
    groupedDf
      .queryExecution
      .toRdd

  rddIRows.foreach(println)
  println

  /** ??? Должен вернуть [0,1,5] [0,0,4], а возвращает [0,0,4] [0,0,4] */
  rddIRows
    .take(2)
    .foreach(println)

  println

  /** С InternalRow всегда рядом держат схему (например в реализации коннекторов). */
  val schema: StructType = groupedDf.schema

  /** ??? Должен вернуть [0,1,5] [0,0,4], а возвращает [0,0,4] [0,0,4] */
  println(rddIRows.collect.mkString("Array(", ", ", ")"))

  val firstIRow: InternalRow =
    rddIRows
      .collect
      .head

  /** ??? Должен вернуть [0,1,5], а возвращает [0,0,4] */
  println(firstIRow)
  println

  /** ??? Должен вернуть 1:java.lang.Integer 5:java.lang.Long, а возвращает 0:java.lang.Integer 4:java.lang.Long */
  firstIRow
    /** Нужно передать схему извне. */
    .toSeq(schema)
    .foreach(el => println(s"$el:${el.getClass.getCanonicalName}"))

  println

  val idx: Int = 0
  /** ??? Должен вернуть 1, а возвращает 0 */
  println(firstIRow.get(idx, schema(idx).dataType))
  println

  /** InternalRow + schema. */
  firstIRow
    .toSeq(schema)
    .zip(schema)
    .foreach(println)

  println

  /** Создание InternalRow. */
  val newData: List[Any] = List("foo", "bar", 0, 1)
  val newIRow: InternalRow = InternalRow.fromSeq(newData)
  println(newIRow)
  println(newIRow.getClass.getCanonicalName)

  /**
   * InternalRow:
   * 1. тип struct - InternalRow.
   * 2. тип array - ArrayData.
   * 3. тип map - MapData.
   *
   * DataFrame:
   * 1. TimestampType - java.lang.Long.
   * 2. LongType - java.lang.Long.
   */

}
