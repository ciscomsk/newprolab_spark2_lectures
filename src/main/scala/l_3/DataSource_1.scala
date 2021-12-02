package l_3

import shared.SparkCommons

object DataSource_1 extends SparkCommons with App {
  println(sc.master)
  println(sc.appName)
  println(sc.uiWebUrl.get)
  println(sc.version)

  /** ЗАПИСЬ данных. */
  /** package org.apache.spark.sql.sources */
  /*
    @Stable
    trait CreatableRelationProvider {
    /**
     * Saves a DataFrame to a destination (using data source-specific parameters)
     *
     * @param sqlContext SQLContext
     * @param mode specifies what happens when the destination already exists
     * @param parameters data source-specific parameters
     * @param data DataFrame to save (i.e. the rows after executing the query)
     * @return Relation with a known schema
     *
     * @since 1.3.0
     */
    def createRelation(
        sqlContext: SQLContext, - аналог SparkSession, который использовался в Spark 1.x
        mode: SaveMode, - Append/Ignore/Overwrite/ErrorIfExists
        parameters: Map[String, String], - список параметров, передаваемый через option()/options()
        data: DataFrame): BaseRelation - DataFrame, который необходимо записать в источник
    }

    /**
     * Data sources should implement this trait so that they can register an alias to their data source.
     * This allows users to give the data source alias as the format type over the fully qualified
     * class name.
     *
     * A new instance of this class will be instantiated each time a DDL call is made.
     *
     * @since 1.5.0
     */
    @Stable
    trait DataSourceRegister {
      /**
       * The string that represents the format that this data source provider uses. This is
       * overridden by children to provide a nice alias for the data source. For example:
       *
       * {{{
       *   override def shortName(): String = "parquet"
       * }}}
       *
       * @since 1.5.0
       */
      def shortName(): String
    }
   */

  /** ЧТЕНИЕ данных. */
  /** package org.apache.spark.sql.sources */
  /**
   * Если при чтении передаем схему (.schema) - вызывается SchemaRelationProvider,
   * если не передаем (можно взять из источника) - RelationProvider.
   *
   * abstract class BaseRelation - задает схему.
   * trait TableScan - получает данные.
   */
  /*
    /**
     * Implemented by objects that produce relations for a specific kind of data source.  When
     * Spark SQL is given a DDL operation with a USING clause specified (to specify the implemented
     * RelationProvider), this interface is used to pass in the parameters specified by a user.
     *
     * Users may specify the fully qualified class name of a given data source.  When that class is
     * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
     * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
     * data source 'org.apache.spark.sql.json.DefaultSource'
     *
     * A new instance of this class will be instantiated each time a DDL call is made.
     *
     * @since 1.3.0
     */
    @Stable
    trait RelationProvider {
      /**
       * Returns a new base relation with the given parameters.
       *
       * @note The parameters' keywords are case insensitive and this insensitivity is enforced
       * by the Map that is passed to the function.
       */
      def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
    }

    /**
     * Implemented by objects that produce relations for a specific kind of data source
     * with a given schema.  When Spark SQL is given a DDL operation with a USING clause specified (
     * to specify the implemented SchemaRelationProvider) and a user defined schema, this interface
     * is used to pass in the parameters specified by a user.
     *
     * Users may specify the fully qualified class name of a given data source.  When that class is
     * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
     * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
     * data source 'org.apache.spark.sql.json.DefaultSource'
     *
     * A new instance of this class will be instantiated each time a DDL call is made.
     *
     * The difference between a [[RelationProvider]] and a [[SchemaRelationProvider]] is that
     * users need to provide a schema when using a [[SchemaRelationProvider]].
     * A relation provider can inherit both [[RelationProvider]] and [[SchemaRelationProvider]]
     * if it can support both schema inference and user-specified schemas.
     *
     * @since 1.3.0
     */
    @Stable
    trait SchemaRelationProvider {
      /**
       * Returns a new base relation with the given parameters and user defined schema.
       *
       * @note The parameters' keywords are case insensitive and this insensitivity is enforced
       * by the Map that is passed to the function.
       */
      def createRelation(
          sqlContext: SQLContext,
          parameters: Map[String, String],
          schema: StructType): BaseRelation
    }

    /**
     * Represents a collection of tuples with a known schema. Classes that extend BaseRelation must
     * be able to produce the schema of their data in the form of a `StructType`. Concrete
     * implementation should inherit from one of the descendant `Scan` classes, which define various
     * abstract methods for execution.
     *
     * BaseRelations must also define an equality function that only returns true when the two
     * instances will return the same data. This equality function is used when determining when
     * it is safe to substitute cached results for a given relation.
     *
     * @since 1.3.0
     */
    @Stable
    abstract class BaseRelation {
      def sqlContext: SQLContext
      def schema: StructType
      ...
    }

    /**
     * A BaseRelation that can produce all of its tuples as an RDD of Row objects.
     *
     * @since 1.3.0
     */
    @Stable
    trait TableScan {
      def buildScan(): RDD[Row]
    }
   */

  /**
   * Для того, чтобы в качестве формата можно было указывать простое имя, необходимо:
   * 1. trait DataSourceRegister
   * 2. Создать файл:src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister
   */



}
