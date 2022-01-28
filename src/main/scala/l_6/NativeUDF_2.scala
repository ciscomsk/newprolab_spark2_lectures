package l_6

/** package org.apache.spark.sql */
/*
  /**
   * (Scala-specific) Converts a column containing a `StructType`, `ArrayType` or
   * a `MapType` into a JSON string with the specified schema.
   * Throws an exception, in the case of an unsupported type.
   *
   * @param e a column containing a struct, an array or a map.
   * @param options options to control how the struct column is converted into a json string.
   *                accepts the same options and the json data source.
   *                See
   *                <a href=
   *                  "https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *                  Data Source Option</a> in the version you use.
   *                Additionally the function supports the `pretty` option which enables
   *                pretty JSON generation.
   *
   * @group collection_funcs
   * @since 2.1.0
   */
  // scalastyle:on line.size.limit
  def to_json(e: Column, options: Map[String, String]): Column = withExpr {
    StructsToJson(options, e.expr)
  }
 */

/** package org.apache.spark.sql */
/*
  ...
  private def withExpr(expr: Expression): Column = Column(expr)
  ...
 */

/** package org.apache.spark.sql.catalyst.expressions */
/*
  case class StructsToJson(
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with CodegenFallback
    with ExpectsInputTypes with NullIntolerant {
    ...

  UnaryExpression - описывает функцию от одной переменной. Если бы функция принимала 2 аргумента - был бы BinaryExpression и т.д.
  TimeZoneAwareExpression - используется для функций работающих со временем.
  CodegenFallback - реализует кодогенерацию так, чтобы она не работала.
  ExpectsInputTypes - реализует валидацию типов входных данных.
 */


/** package org.apache.spark.sql */
/*
  /**
   * Aggregate function: returns the number of items in a group.
   *
   * @group agg_funcs
   * @since 1.3.0
   */
  def count(e: Column): Column = withAggregateFunction {
    e.expr match {
      // Turn count(*) into count(1)
      case s: Star => Count(Literal(1))
      case _ => Count(e.expr)
    }
  }
 */