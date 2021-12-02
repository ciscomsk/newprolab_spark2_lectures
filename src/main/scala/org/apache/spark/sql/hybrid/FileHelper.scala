package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.{BufferedWriter, File, FileWriter}
import scala.annotation.tailrec
import scala.io.{BufferedSource, Source}

object FileHelper {
  def getFiles(path: String): Array[File] = {
    val basePath: File = new File(path)

    def getFilesRec(recPath: File): Array[File] =
      if (recPath.isDirectory) recPath.listFiles.flatMap(getFilesRec)
      else Array(recPath)

    getFilesRec(basePath)
  }

  def write(path: String, data: Iterator[String]): Unit = {
    val file: File = new File(path)
    val bw: BufferedWriter = new BufferedWriter(new FileWriter(file))

    @tailrec
    def iterTraversal[T](iter: Iterator[T]): Unit =
      if (iter.hasNext) {
        bw.write(data.next)
        bw.write("\n")

        iterTraversal(iter)
      }

    iterTraversal(data)
    bw.close()
  }

  def ensureDirectory(path: String): Unit = {
    this.synchronized {
      val dir: File = new File(path)
      dir.mkdir
    }
  }

  /** !!! Схама в InternalRow не энкапсулирована. */
  /** Вынесли из класса HybridRelation в объект т.к. java.io.NotSerializableException: org.apache.spark.sql.hybrid.HybridRelation */
  def toCsv(iter: Iterator[InternalRow], schema: StructType): Iterator[String] =
    iter.map { iRow =>
      val seq: Seq[Any] = iRow.toSeq(schema)
      val zipped: Seq[(Any, DataType)] = seq.zip(schema.map(_.dataType))

      val stringSeq: Seq[String] = zipped.map {
        case (null, _) =>
          ""

        case (v: java.lang.Long, _: LongType) =>
          v.toString

        case (v: UTF8String, _: StringType) =>
          v.toString

        case (v: java.lang.Integer, _: IntegerType) =>
          v.toString

        case (v, t) =>
          throw new UnsupportedOperationException(s"$v of type ${t.simpleString} is not supported!")
      }

      stringSeq.mkString(",")
    }

  /** PrunedScan. */
  def fromCsv(filePath: String, schema: StructType, requiredColumns: Array[String]): Iterator[InternalRow] = {
    /** Схема полностью совпадает с выбранными колонками. */
    if (schema.fieldNames.toList == requiredColumns.toList) fromCsv(filePath, schema)
    else {
      val file: BufferedSource = Source.fromFile(filePath)
      val lines = file.getLines

      lines.map { line =>
        /** 1, hello world, 0 => Array(1, hello world, 0) */
        val split: Array[String] = line.split(",", -1)
        if (split.length != schema.length) throw new IllegalArgumentException(s"Schema does not match: ${schema.simpleString}")

        /** Array(1, hello world, 0) zip Array("id": LongType, "value": StringType, "part_id": IntegerType */
        val zipped: Array[(String, DataType)] = split.zip(schema.map(_.dataType))

        /** "value", "id" */
        val projected: Array[(String, DataType)] =
          requiredColumns
            .map { colName =>
              /** "value" => 1, "id" => 0 */
              val colIdx: Int = schema.fieldIndex(colName)
              zipped(colIdx)
            }

        val typedDate: Array[Any] = projected.map {
          case (s, LongType) => java.lang.Long.valueOf(s.toLong)
          case (s, IntegerType) => java.lang.Integer.valueOf(s.toInt)
          case (s, StringType) => UTF8String.fromString(s)
          case (s, dt) => throw new UnsupportedOperationException(s"$s of type ${dt.simpleString} is not supported!")
        }

        InternalRow.fromSeq(typedDate)
      }
    }
  }

  /** TableScan. */
  def fromCsv(filePath: String, schema: StructType): Iterator[InternalRow] = {
    val file: BufferedSource = Source.fromFile(filePath)
    val lines = file.getLines

    lines.map { line =>
      /** limit = -1 - достаем даже пустые строки - ,, */
      val split: Array[String] = line.split(",", -1)
      if (split.length != schema.length) throw new IllegalArgumentException(s"Schema does not match: ${schema.simpleString}")

      val zipped: Array[(String, DataType)] = split.zip(schema.map(_.dataType))

      val typedDate: Array[Any] = zipped.map {
        case (s, LongType) => java.lang.Long.valueOf(s.toLong)
        case (s, IntegerType) => java.lang.Integer.valueOf(s.toInt)
        case (s, StringType) => UTF8String.fromString(s)
        case (s, dt) => throw new UnsupportedOperationException(s"$s of type ${dt.simpleString} is not supported!")
      }

      InternalRow.fromSeq(typedDate)
    }
  }

}
