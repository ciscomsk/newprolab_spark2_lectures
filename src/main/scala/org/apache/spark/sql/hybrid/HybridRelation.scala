package org.apache.spark.sql.hybrid

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider, TableScan}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.File
import java.lang

/** Класс инстанциируется на драйвере. */
class HybridRelation extends CreatableRelationProvider
  with RelationProvider
  with SchemaRelationProvider
  with DataSourceRegister
  with Logging {

  override def shortName(): String = "hybrid-csv"

  /** Выполняет запись датафрейма. */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    log.info(s"${this.logName} has been created.")
    log.info(s"SaveMode=$mode")
    log.info(s"${parameters.mkString(", ")}")
    log.info(s"${data.schema.simpleString}")

    /** 1. Получаем InternalRow. */
    val iRdd: RDD[InternalRow] =
      data
        .queryExecution
        .toRdd

    /** Получение пути для записи. */
    val saveDirectory: String = parameters.getOrElse("path", throw new IllegalArgumentException("path must be set"))
    /** Создание директории для записи датафрейма. */
    FileHelper.ensureDirectory(saveDirectory)

    val schema: StructType = data.schema

    /** Проверяем, что нет сложных типов данных - struct/array/map. */
    schema
      .map(_.dataType)
      .find {
        case _: StructType => true
        case _: MapType => true
        case _: ArrayType => true
        case _ => false
      }
      .foreach { dataType =>
        throw new UnsupportedOperationException(s"${dataType.simpleString} is not supported!")
      }

    /**
     * 2. Запись
     * foreachPartition(f) - функция f выполняется на воркерах.
     */
    iRdd.foreachPartition { partition =>  // Iterator[InternalRow]
      val stringIter: Iterator[String] = FileHelper.toCsv(partition, schema)
      /** У каждой партиции будет случайный uuid. */
      val filePath: String = s"$saveDirectory/${java.util.UUID.randomUUID.toString}.csv"

      FileHelper.write(filePath, stringIter)
    }

    /** Возвращаем пустой BaseRelation - т.к. в функции не используется (аналогично сделано в коннекторе кафки). */
    new BaseRelation {
      override def sqlContext: SQLContext = ???
      override def schema: StructType = ???
    }
  }


  /** Выполняет чтение датафрейма без заданной схемы. */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    throw new UnsupportedOperationException("Schema must be specified!")

  /** Выполняет чтение датафрейма c заданной схемой. */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {

    log.info(s"createRelation call.")

    new HybridBaseRelation(parameters ,schema)
  }

}

class HybridBaseRelation(parameters: Map[String, String], usedSchema: StructType) extends BaseRelation
  with TableScan
  with Logging {

  log.info(s"${this.logName} has been created.")

  override def sqlContext: SQLContext =
    SparkSession
      .active
      .sqlContext

  override def schema: StructType = usedSchema

  /** Для того, чтобы работать с InternalRow, а не автоматической конвертаций в Row. */
  override def needConversion: Boolean = false

  override def buildScan(): RDD[Row] = {
    log.info("buildScan call.")

    /** needConversion = false - InternalRow => Row */
    new CsvRdd(parameters, schema).asInstanceOf[RDD[Row]]
  }

}

class CsvRdd(parameters: Map[String, String], schema: StructType) extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) with Logging {
  val readDirectory: String = parameters.getOrElse("path", throw new IllegalArgumentException("path must be set"))
  val files: Array[File] = FileHelper.getFiles(readDirectory).filter(file => file.isFile && file.getName.endsWith("csv"))

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val csvPartition: CsvPartition = split.asInstanceOf[CsvPartition]
    FileHelper.fromCsv(csvPartition.path, schema)
  }

  override protected def getPartitions: Array[Partition] =
    files
      .zipWithIndex
      .map {
        case (file, idx) => CsvPartition(file.getAbsolutePath, idx)
      }
}

case class CsvPartition(path: String, index: Int) extends Partition