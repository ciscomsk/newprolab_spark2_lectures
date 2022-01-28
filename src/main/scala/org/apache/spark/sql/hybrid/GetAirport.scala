package org.apache.spark.sql.hybrid

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{input_file_name, udf}
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType, StructField, StructType, TimestampType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.{BsonString, BsonValue}
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Filters

import java.lang
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

case class Airport(ident: String, `type`: String, name: String, iso_country: String)

/** !!! Object - создается 1 раз на воркер. */
object GetAirport extends Logging {
  /** !!! Будет распечатано только 1 раз на каждом воркере. */
  log.info(">> GetAirport call <<")

  private val mongoUriRef: String = "MONGO_URI"

  private val mongoUri: String =
    sys.env.getOrElse(mongoUriRef, "mongodb://localhost:27017")

  private lazy val codecRegistry: CodecRegistry =
    fromRegistries(fromProviders(classOf[Airport]), DEFAULT_CODEC_REGISTRY)

  @transient
  lazy val mongoClient: MongoClient = MongoClient(mongoUri)
  lazy val mongoDatabase: MongoDatabase = mongoClient.getDatabase("test").withCodecRegistry(codecRegistry)

  lazy val airports: MongoCollection[Airport] = mongoDatabase.getCollection("airports")

  /** Для native udf. */
  lazy val airportsRaw: MongoCollection[Document] = mongoDatabase.getCollection("airports")

  def getAirport(ident: String): Option[Airport] = {
    val filter: Bson = Filters.eq("_id", ident)

    val futureDoc: Future[Option[Airport]] =
      airports
        .find(filter)
        /** Фильтр может вернуть несколько документов. */
        .collect()
        .toFuture()
        /** Точно знаем, что результат будет 1. */
        .map(_.headOption)

    val doc: Option[Airport] = Await.result(futureDoc, 10.seconds)

    doc
  }

  /** Для native udf. */
  def getAirportRaw(ident: String): Option[Map[String, BsonValue]] = {
    val filter: Bson = Filters.eq("_id", ident)

    val futureDoc: Future[Option[Document]] =
      airportsRaw
        .find(filter)
        .collect()
        .toFuture()
        .map(_.headOption)

    val doc: Option[Document] = Await.result(futureDoc, 10.seconds)

    doc.map(_.toMap)
  }

  def getAirportUdf: UserDefinedFunction = udf {
    (ident: String) => getAirport(ident)
  }

  /** apply в private[sql] object Column. */
  def withExpr(expr: Expression): Column = Column(expr)

//  def to_json(e: Column, options: Map[String, String]): Column = withExpr {
//    StructsToJson(options, e.expr)
//  }

  def getAirportNativeUdf(e: Column, fields: List[String]): Column = withExpr {
//    GetAirportNative(e.expr, fields)

    val schema: StructType = StructType { fields.map(field => StructField(field, StringType)) }
    GetAirportNative(e.expr, schema)
  }
}

//case class GetAirportNative(child: Expression, fields: List[String])
case class GetAirportNative(child: Expression, schema: StructType)
  extends UnaryExpression with CodegenFallback with ExpectsInputTypes with Logging {

  println(s"GetAirportNative has been created.")

//  override def dataType: DataType = {
//    println(s">> dataType call <<")
//    StructType { fields.map(field => StructField(field, StringType)) }
//  }
  /** => можно облегчить - вынести схему в конструктор кейс класса. */
  override def dataType: DataType = {
    println(s">> dataType call <<")
    schema
  }

  override protected def withNewChildInternal(newChild: Expression): GetAirportNative =
    copy(child = newChild)

  /** Вызывается на каждую строку. */
  override protected def nullSafeEval(input: Any): Any = {
    /*
      input:
      UTF8String/java.lang.Integer/...
      InternalRow
      ArrayData
      MapData
     */

    val ident: String = input.asInstanceOf[UTF8String].toString

    /*
      Some(Map(coordinates -> BsonString{value='-52.36, -24.61'},
      name -> BsonString{value='Fazenda Sorte Grande Airport'},
      iso_country -> BsonString{value='BR'},
      iso_region -> BsonString{value='BR-PR'},
      _id -> BsonString{value='SSWB'},
      continent -> BsonString{value='SA'}, ident -> BsonString{value='SSWB'},
      gps_code -> BsonString{value='SSWB'}, elevation_ft -> BsonInt32{value=2339},
      type -> BsonString{value='closed'}, municipality -> BsonString{value='Roncador'}))
     */
    val doc: Option[Map[String, BsonValue]] = GetAirport.getAirportRaw(ident)

    doc match {
      case Some(value) =>
        val arr: Array[UTF8String] = schema.fieldNames.map { field =>
          val data: BsonValue = value(field)

          data match {
            case v: BsonString => UTF8String.fromString(v.getValue)
            case v => throw new UnsupportedOperationException(s"Type $v is not supported!")
          }
        }
        InternalRow.fromSeq(arr)

      case None =>
        null
    }
  }

  /**
   * Валидация типа - выполняется на этапа Analyzed Logical Plan.
   * Можно не ограничивать тип - не имплементировать ExpectsInputTypes.
   */
  override def inputTypes: Seq[AbstractDataType] = {
    println(s">> inputTypes call <<")
    List(TypeCollection(StringType))
  }
}

object TimeFunctions {
  def withExpr(expr: Expression): Column = Column(expr)

  def addHours(e: Column, hours: java.lang.Integer): Column = withExpr {
    AddHours(e.expr, hours)
  }

}

case class AddHours(child: Expression, hours: java.lang.Integer)
  extends UnaryExpression with CodegenFallback with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = List(TypeCollection(TimestampType))

  /** !!! asNullable (не нужен ?) + def nullable => не будет падать если возвращаем null.  */
  override def dataType: DataType = TimestampType
  override def nullable: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): AddHours =
    copy(child = newChild)

  override protected def nullSafeEval(input: Any): Any = {
    val ts: lang.Long = input.asInstanceOf[java.lang.Long]
    println(ts) // 1 639 393 806 620 287 - формат в наносекундах

    /** !!! без Long - будет переполнение. */
//    ts + (hours.toLong * 3600L * 1000000L)

    null
  }
}