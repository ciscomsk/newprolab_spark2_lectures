package org.apache.spark.sql.hybrid

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Sink, Source}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, EqualTo, Filter, IsNotNull, PrunedFilteredScan, PrunedScan, RelationProvider, SchemaRelationProvider, StreamSinkProvider, StreamSourceProvider, StringContains, TableScan}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

import java.io.File

/** Класс инстанциируется на драйвере. */
class HybridRelation extends CreatableRelationProvider
  with RelationProvider
  with SchemaRelationProvider
  with DataSourceRegister
  with StreamSinkProvider
  with StreamSourceProvider
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

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {

    log.info("createSink call.")
    log.info(s"Parameters=$parameters")
    log.info(s"Partition columns=$partitionColumns")
    log.info(s"Output mode=$outputMode")

    new HybridSink(parameters)
  }

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {

    log.info(">> sourceSchema call <<")
    log.info(s"Schema=${schema.map(_.toDDL).mkString}")
    log.info(s"Provider name=$providerName")
    log.info(s"Parameters=$parameters")

    val thisSchema: StructType = schema.getOrElse(throw new IllegalArgumentException("Schema must be provided!"))

    /** Логики - нет, просто следуем сигнатуре функции. */
    providerName -> thisSchema
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {

    log.info(">> createSource call <<")
    log.info(s"Metadata path=$metadataPath")
    log.info(s"Schema=${schema.map(_.toDDL).mkString}")
    log.info(s"Provider name=$providerName")
    log.info(s"Parameters=$parameters")

    val thisSchema: StructType = schema.getOrElse(throw new IllegalArgumentException("Schema must be provided!"))

    new HybridSource(thisSchema, parameters)
  }
}

class HybridBaseRelation(parameters: Map[String, String], usedSchema: StructType) extends BaseRelation
//  with TableScan
//  with PrunedScan
  with PrunedFilteredScan
  with Logging {

  log.info(s"${this.logName} has been created.")

  override def sqlContext: SQLContext =
    SparkSession
      .active
      .sqlContext

  override def schema: StructType = usedSchema

  /** Для того, чтобы работать с InternalRow, а не автоматической конвертаций в Row. */
  override def needConversion: Boolean = false

  /** TableScan. */
//  override def buildScan(): RDD[Row] = {
//    log.info("buildScan call: TableScan.")
//
//    /** needConversion = false - InternalRow => Row */
//    new CsvRdd(parameters, schema).asInstanceOf[RDD[Row]]
//  }

  /** PrunedScan. */
//  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
//    log.info("buildScan call: PrunedScan.")
//    log.info(s"${requiredColumns.mkString("Array(", ", ", ")")}")
//    log.info(s"${schema.simpleString}")
//
//    new CsvRdd(parameters, schema, requiredColumns).asInstanceOf[RDD[Row]]
//  }

  /** PrunedFilteredScan. */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    log.info("buildScan call: PrunedFilteredScan.")
    log.info(s"Required columns: ${requiredColumns.mkString("Array(", ", ", ")")}")
    log.info(s"Schema: ${schema.simpleString}")
    log.info(s"Used filters: ${filters.mkString("Array(", ", ", ")")}")

    val pushedFilters: Array[Filter] = filters.diff(unhandledFilters(filters))
    log.info(s"Pushed filters: ${pushedFilters.mkString("Array(", ", ", ")")}")

    new CsvRdd(parameters, schema, requiredColumns, pushedFilters).asInstanceOf[RDD[Row]]
  }

  /** Определяем неподдерживаемые (на уровне источника) фильтры. */
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    log.info("UnhandledFilters call.")

    filters
      .filter {
        case IsNotNull(_) => false
        case EqualTo(_, _) => false
        case StringContains(_, _) => false
        case _ => true
      }
  }
}

class CsvRdd(parameters: Map[String, String],
             schema: StructType,
             requiredColumns: Array[String],
             pushedFilters: Array[Filter]) extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) with Logging {

  val readDirectory: String = parameters.getOrElse("path", throw new IllegalArgumentException("path must be set"))
  val files: Array[File] = FileHelper.getFiles(readDirectory).filter(file => file.isFile && file.getName.endsWith("csv"))

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val csvPartition: CsvPartition = split.asInstanceOf[CsvPartition]
    FileHelper.fromCsv(csvPartition.path, schema, requiredColumns, pushedFilters)
  }

  override protected def getPartitions: Array[Partition] =
    files
      .zipWithIndex
      .map {
        case (file, idx) => CsvPartition(file.getAbsolutePath, idx)
      }
}

case class CsvPartition(path: String, index: Int) extends Partition

class HybridSink(parameters: Map[String, String]) extends Sink with Logging {
  /** data - стримовый датафрейм. */
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    log.info(s"BatchId=$batchId")
    log.info(s"Schema=${data.schema.toDDL}")

    // err - т.к. датафрейм стримовый
//    data.show()

    val schema: StructType = data.schema

    val iRdd: RDD[InternalRow] =
      data
        .queryExecution
        .toRdd

    val saveDirectory: String = parameters.getOrElse("path", throw new IllegalArgumentException("Please specify path!"))
    FileHelper.ensureDirectory(saveDirectory)

    /** Запись взяли из class HybridRelation - def createRelation - iRdd.foreachPartition */
    iRdd.foreachPartition { partition =>
      val stringIter: Iterator[String] = FileHelper.toCsv(partition, schema)
      val filePath: String = s"$saveDirectory/${java.util.UUID.randomUUID.toString}.csv"

      FileHelper.write(filePath, stringIter)
    }
  }
}

class HybridSource(sourceSchema: StructType, parameters: Map[String, String]) extends Source with Logging {
  log.info("HybridSource has been created.")

  val readDirectory: String = parameters.getOrElse("path", throw new IllegalArgumentException("Please specify path!"))

  private val spark: SparkSession = SparkSession.active
//  private val sc: SparkContext = spark.sparkContext

  override def schema: StructType = {
    log.info(">> schema call <<")
    sourceSchema
  }

  override def getOffset: Option[Offset] = {
    /**
     * None - стрим пуст (никогда не запускался).
     * Some(n + 1), если n + 1 == n - в стриме нет новых данных - getBatch вызван не будет.
     * Some(n + 1), если n + 1 != n - есть новые данные в стриме, вызываем getBatch.
     */

    log.info(">> getOffset call <<")

    /** Взяли из class csvRdd. */
    val files: Array[File] =
      FileHelper.getFiles(readDirectory)
        .filter(file => file.isFile && file.getName.endsWith("csv"))
        /** !!! сортировка - нужна для однозначного сравнения офсетов (соблюдение одинакового порядка в листинге фалов) */
        .sorted

    val offset: Some[HybridOffset] =  Some(
      HybridOffset(files.map(_.getAbsolutePath).toList)
    )

    log.info(s"New offset=$offset")

    offset
  }

  private def getFilesFromOffset(offset: Offset): List[String] =
    offset
      .json()
      /** c /n - ошибка */
      .split(",")
      .toList

  def getDiff(start: Option[Offset], end: Offset): List[String] = {
    start match {
      case None =>
        getFilesFromOffset(end)

      case Some(offset) =>
        getFilesFromOffset(end).diff { getFilesFromOffset(offset) }
    }
  }

  /** !!! getBatch должен вернуть стримовый датафрейм. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    log.info(">> getBatch call <<")
    log.info(s"Offsets: start=$start, end=$end")

    val filesToRead: List[String] = getDiff(start, end)
    val iRdd: RDD[InternalRow] = new StreamCsvRdd(filesToRead, sourceSchema)
    log.info(s"RDD has been created, numParts=${iRdd.getNumPartitions}")

    val streamDf: DataFrame = spark.internalCreateDataFrame(iRdd, sourceSchema, isStreaming = true)
    log.info(s"Dataframe has been created.")

    streamDf
  }

  override def stop(): Unit = {}
}

case class HybridOffset(files: List[String]) extends Offset {
  /** c /n - ошибка */
  override def json(): String = files.mkString(",")
}

class StreamCsvRdd(files: List[String], schema: StructType) extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) with Logging {
  /** !!!Для того, чтобы сериализовалось только это поле, а не весь класс. */
  val thisSchema: StructType = schema.copy()

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val csvPartition: CsvPartition = split.asInstanceOf[CsvPartition]
    FileHelper.fromCsv(csvPartition.path, thisSchema)
  }


  override protected def getPartitions: Array[Partition] =
    files
      .zipWithIndex
      .map {
        case (file, idx) => CsvPartition(file, idx)
      }
      .toArray
}