package l_5

class StreamingSource_1 {
  /**
   * StreamSinkProvider - подмешивание данного трейта позволяет использовать класс
   * в качестве синка для Streaming DataFrame.
   */
  /** package org.apache.spark.sql.sources */
  /*
    /**
     * ::Experimental::
     * Implemented by objects that can produce a streaming `Sink` for a specific format or system.
     *
     * @since 2.0.0
     */
    @Unstable
    trait StreamSinkProvider {
      def createSink(
          sqlContext: SQLContext,
          parameters: Map[String, String],
          partitionColumns: Seq[String],
          outputMode: OutputMode): Sink
    }

    sqlContext - аналог SparkSession, который использовался в Spark1.
    parameters - параметры, указанные в .option()/options().
    partitionColumns - список колонок, указанных в .partitionBy().
    outputMode - режим записи - append/complete/update.

    createSink - отвечает за создание Sink, в котором реализуется логика записи батча.
   */

  /** Sink - трейт отвечает за запись батчей в синк. */
  /** package org.apache.spark.sql.execution.streaming */
  /*
    /**
     * An interface for systems that can collect the results of a streaming query. In order to preserve
     * exactly once semantics a sink must be idempotent in the face of multiple attempts to add the same
     * batch.
     *
     * Note that, we extends `Table` here, to make the v1 streaming sink API be compatible with
     * data source v2.
     */
    trait Sink extends Table {

      /**
       * Adds a batch of data to this sink. The data for a given `batchId` is deterministic and if
       * this method is called more than once with the same batchId (which will happen in the case of
       * failures), then `data` should only be added once.
       *
       * Note 1: You cannot apply any operators on `data` except consuming it (e.g., `collect/foreach`).
       * Otherwise, you may get a wrong result.
       *
       * Note 2: The method is supposed to be executed synchronously, i.e. the method should only return
       * after data is consumed by sink successfully.
       */
      def addBatch(batchId: Long, data: DataFrame): Unit

      override def name: String = {
        throw new IllegalStateException("should not be called.")
      }

      override def schema: StructType = {
        throw new IllegalStateException("should not be called.")
      }

      override def capabilities: util.Set[TableCapability] = {
        throw new IllegalStateException("should not be called.")
      }
    }

    batchId - номер батча.
    data - DataFrame с данными и флагом isStreaming = true.

    addBatch - метод, применяемый к каждому батчу для записи в синк.
   */


  /**
   * StreamSourceProvider - подмешивание данного трейта позволяет использовать класс
   * в качестве источника для Streaming DataFrame.
   */
  /** package org.apache.spark.sql.sources */
  /*
    /**
     * ::Experimental::
     * Implemented by objects that can produce a streaming `Source` for a specific format or system.
     *
     * @since 2.0.0
     */
    @Unstable
    trait StreamSourceProvider {

      /**
       * Returns the name and schema of the source that can be used to continually read data.
       * @since 2.0.0
       */
      def sourceSchema(
          sqlContext: SQLContext,
          schema: Option[StructType],
          providerName: String,
          parameters: Map[String, String]): (String, StructType)

      /**
       * @since 2.0.0
       */
      def createSource(
          sqlContext: SQLContext,
          metadataPath: String,
          schema: Option[StructType],
          providerName: String,
          parameters: Map[String, String]): Source
    }

    providerName - название формата, указанного в .format().
    schema - схема, указанная в .schema().
    parameters - параметры, указанные в .option/options().
    metadataPath - путь чекпоинта, указанный в checkpointLocaton.
    sqlContext - аналог SparkSession, который использовался в Spark1.

    createSource - отвечает за создание Source, реализующего логику чтения стрима из источника.
    sourceSchema - предназначение неизвестно, но его нужно реализовывать.
   */

  /** Source - трейт отвечает за формирование батчей на основании источника. */
  /** package org.apache.spark.sql.execution.streaming */
  /*
    /**
     * A source of continually arriving data for a streaming query. A [[Source]] must have a
     * monotonically increasing notion of progress that can be represented as an [[Offset]]. Spark
     * will regularly query each [[Source]] to see if any more data is available.
     *
     * Note that, we extends `SparkDataStream` here, to make the v1 streaming source API be compatible
     * with data source v2.
     */
    trait Source extends SparkDataStream {

      /** Returns the schema of the data from this source */
      def schema: StructType

      /**
       * Returns the maximum available offset for this source.
       * Returns `None` if this source has never received any data.
       */
      def getOffset: Option[Offset]

      /**
       * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None`,
       * then the batch should begin with the first record. This method must always return the
       * same data for a particular `start` and `end` pair; even after the Source has been restarted
       * on a different node.
       *
       * Higher layers will always call this method with a value of `start` greater than or equal
       * to the last value passed to `commit` and a value of `end` less than or equal to the
       * last value returned by `getOffset`
       *
       * It is possible for the [[Offset]] type to be a [[SerializedOffset]] when it was
       * obtained from the log. Moreover, [[StreamExecution]] only compares the [[Offset]]
       * JSON representation to determine if the two objects are equal. This could have
       * ramifications when upgrading [[Offset]] JSON formats i.e., two equivalent [[Offset]]
       * objects could differ between version. Consequently, [[StreamExecution]] may call
       * this method with two such equivalent [[Offset]] objects. In which case, the [[Source]]
       * should return an empty [[DataFrame]]
       */
      def getBatch(start: Option[Offset], end: Offset): DataFrame

      /**
       * Informs the source that Spark has completed processing all data for offsets less than or
       * equal to `end` and will only request offsets greater than `end` in the future.
       */
      def commit(end: Offset) : Unit = {}

      override def initialOffset(): OffsetV2 = {
        throw new IllegalStateException("should not be called.")
      }

      override def deserializeOffset(json: String): OffsetV2 = {
        throw new IllegalStateException("should not be called.")
      }

      override def commit(end: OffsetV2): Unit = {
        throw new IllegalStateException("should not be called.")
      }
    }

    schema - возвращает схему данных из источника.
    getOffset - вызывается перед каждым батчем и проверяет, есть ли новые данные в источнике.
    getBatch - функция чтения батча из источника от start до end.
    commit - вызывается после успешной записи батча в синк. Опциональный.
    stop - вызывается после остановке стрима. Из SparkDataStream. Опциональный.
   */

  /** package org.apache.spark.sql.connector.read.streaming */
  /*
    /**
     * An abstract representation of progress through a {@link MicroBatchStream} or
     * {@link ContinuousStream}.
     * <p>
     * During execution, offsets provided by the data source implementation will be logged and used as
     * restart checkpoints. Each source should provide an offset implementation which the source can use
     * to reconstruct a position in the stream up to which data has been seen/processed.
     *
     * @since 3.0.0
     */
    @Evolving
    public abstract class Offset {
        /**
         * A JSON-serialized representation of an Offset that is
         * used for saving offsets to the offset log.
         * <p>
         * Note: We assume that equivalent/equal offsets serialize to
         * identical JSON strings.
         *
         * @return JSON string encoding
         */
        public abstract String json();

        /**
         * Equality based on JSON string representation. We leverage the
         * JSON representation for normalization between the Offset's
         * in deserialized and serialized representations.
         */
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Offset) {
                return this.json().equals(((Offset) obj).json());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return this.json().hashCode();
        }

        @Override
        public String toString() {
            return this.json();
        }
    }
   */
}
