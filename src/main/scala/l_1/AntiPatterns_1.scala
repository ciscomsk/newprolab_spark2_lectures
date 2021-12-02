package l_1

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions.{count, monotonically_increasing_id, row_number, udf}
import shared.SparkCommons

import java.lang

object AntiPatterns_1 extends SparkCommons with App {
  import spark.implicits._

  println(sc.master)
  println(sc.appName)
  println(sc.uiWebUrl.get)
  println(sc.version)
  println

  /**
   * 1. Неявный вызов оператора Exchange SinglePartition.
   * !!! Собирает данные всего DataFrame в одну партицию на один из воркеров. Снижает параллелизм до 1 партиции.
   * ~ collect на драйвер.
   *
   * monotonically_increasing_id - монотонно нумерует строки в рамках партиции, между партициями  - будет разрыв нумерации.
   */
  val df: Dataset[lang.Long] =
    spark
      .range(0, 1000, 1, 10)
      /**
       * localCheckpoint - неленивый кэш (+ удаляет план выполнения),
       * но unpersist не работает для localCheckpoint - Spark сам решает, когда очистить память.
       */
      .localCheckpoint

  val wholeDatasetWindow: WindowSpec =
    Window
      .partitionBy()
      .orderBy($"id".asc)

  val projection1: DataFrame =
    df
      .select(
        $"id",
        count("*").over(wholeDatasetWindow)
      )

  projection1.prettyExplain()
  projection1.explain
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Window [count(1) windowspecdefinition(id#0L ASC NULLS FIRST, specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS count(1) OVER (ORDER BY id ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)#7L], [id#0L ASC NULLS FIRST]
       +- Sort [id#0L ASC NULLS FIRST], false, 0
          +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#12]
             +- Scan ExistingRDD[id#0L]
   */

  val projection2: DataFrame =
    df
      .select(
        $"id",
        row_number.over(wholeDatasetWindow)
      )

  projection2.prettyExplain()
  projection2.explain
  /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Window [row_number() windowspecdefinition(id#0L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS row_number() OVER (ORDER BY id ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)#11], [id#0L ASC NULLS FIRST]
       +- Sort [id#0L ASC NULLS FIRST], false, 0
          +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [id=#23]
             +- Scan ExistingRDD[id#0L]
   */

  val projection3: DataFrame =
    df
      .select(
        $"id",
        monotonically_increasing_id
      )

  projection3.prettyExplain()
  projection3.explain
  /*
    == Physical Plan ==
    *(1) Project [id#0L, monotonically_increasing_id() AS monotonically_increasing_id()#14L]
    +- *(1) Scan ExistingRDD[id#0L]
   */

  projection3
    .sample(0.1)
    .show(30)


  /**
   * 2. Partial Caching.
   * Кеширование применяется только к тем партициям, которые были посчитаны одним из действий.
   * !!! Это может привести к тому, что часть данных будет прочитана из кэша (старые данные),
   * а другая часть - из источника (который уже мог изменить свои данные - новые данные) == НЕКОНСИСТЕНТНОСТЬ.
   */
  val df2: Dataset[lang.Long] = spark.range(0, 10000, 1, 100)
  df2.cache
  /**
   * show всегда берет 1 партицию (если в ней хватает данных) => sparkUI => Storage - Fraction Cached(%)
   * Аналогично show работают - фильтры c partition pruning/take.
   */
  df2.show(1)

  /** ??? Если попытаться создать датафрейм равный df2 - он будет ссылаться на существующий. */
  val df3: Dataset[lang.Long] = spark.range(0, 20000, 1, 100)
  df3.cache
  /** После cache должно следоват действие рассчитывающее все партиции - например count. */
  df3.count

  /** Программное отображение Storage. */
  sc
    .getRDDStorageInfo
    .foreach { crdd =>
      println(s"${crdd.name.trim}: ${crdd.numCachedPartitions}/${crdd.numPartitions}")
      println("####")
    }

  println

  val df4: Dataset[lang.Long] = spark.range(0, 15000, 1, 100)
  df4.localCheckpoint
  /** !!! Повторный localCheckpoint создает дополнительную запись в кэш, а не обновляет существующую. */
  df4.localCheckpoint
  df4.localCheckpoint
  /** !!! unpersist не очищает localCheckpoint. */
  df4.unpersist

  /** !!! Повторный cache не создает дополнительную запись в кэш, а обновляет существующую. */
  (1 to 3).foreach { _ =>
    val df: Dataset[lang.Long] = spark.range(0, 5000, 1, 100)
    df.cache
    df.count
  }

  /** Полная очистка кэша, на localCheckpoint не распространяется. */
  spark
    .sharedState
    .cacheManager
    .clearCache


  /** !!! 3. Coalesce может быть незаметно перемещен в начало физического плана. */
  val udf_sleep: UserDefinedFunction = udf { () => Thread.sleep(1000); null }

  val df5: Dataset[lang.Long] = spark.range(0, 10, 1, 2)

//  spark.time {
//    df5
//      .select(udf_sleep())
//      .collect
//  } // 5080 ms

//  spark.time {
    /**
     * !!! Несмотря на план выполнения - первой будет выполнена операция coalesce(1)
     * => юдф будет применен к одной партиции. coalesce не может подняться выше операций до шафла (не может перейти на другой stage - решение 2).
     */
//    val dataDf =
//      df5
//        .select(udf_sleep())
//        .coalesce(1)
//
//    dataDf.prettyExplain()
//    dataDf.explain
    /*
      == Physical Plan ==
      Coalesce 1
      +- *(1) Project [UDF() AS UDF()#213]
         +- *(1) Range (0, 10, step=1, splits=2)
     */
//    dataDf.collect
//  } // 10093 ms

  /** Решение 1 - repartition. */
//  spark.time {
//    val dataDf =
//      df5
//        .select(udf_sleep())
//        .repartition(1)
//
//    dataDf.prettyExplain()
//    dataDf.explain
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Exchange SinglePartition, REPARTITION_BY_NUM, [id=#244]
         +- Project [UDF() AS UDF()#213]
            +- Range (0, 10, step=1, splits=2)
     */
//    dataDf.collect
//  } // 5140 ms

  /** Решение 2 - repartition.сoalesce - coalesce будет пропущен (sparkUI - skipped). */
//  spark.time {
////    val dataDf =
////      df5
////        .select(udf_sleep())
////        .repartition(1)
////        .coalesce(1)
//
    /** Репартицирование может быть неявным - groupBy/join/оконная функция. */
//    val dataDf =
//      df5
//        .select(udf_sleep().as("f"))
//        .groupBy($"f")
//        .count
//        .coalesce(1)
//
//    dataDf.prettyExplain()
//    dataDf.explain
    /** .repartition(1) */
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Exchange SinglePartition, REPARTITION_BY_NUM, [id=#244]
         +- Project [UDF() AS UDF()#213]
            +- Range (0, 10, step=1, splits=2)
     */
    /** .groupBy($"f") */
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Coalesce 1
         +- HashAggregate(keys=[f#213], functions=[count(1)])
            +- Exchange hashpartitioning(f#213, 200), ENSURE_REQUIREMENTS, [id=#253]
               +- HashAggregate(keys=[f#213], functions=[partial_count(1)])
                  +- Project [UDF() AS f#213]
                     +- Range (0, 10, step=1, splits=2
     */
//    dataDf.collect
//  } // 5107 ms  // 5251 ms

  /** Решение 3 - кэширование. */
//  spark.time {
//    val dataDf =
//      df5
//        .select(udf_sleep())
        /** Рекомендуется использовать cache; count */
//        .localCheckpoint
//        .coalesce(1)
//
//    dataDf.prettyExplain()
//    dataDf.explain
    /*
      == Physical Plan ==
      Coalesce 1
      +- *(1) Scan ExistingRDD[UDF()#213]
     */
//    dataDf.collect
//  } // 5113 ms

  /**
   * Лучшее решение - 3 с cache; count - быстрое, но нагружает оперативную память.
   * Решение с repartition - нагружает сетевую/дисковую подсистемы - медленнее, но не нагружает оперативную память.
   */


  val testDf1: Dataset[lang.Long] = spark.range(0, 10, 1, 2)
  df.cache
  df.count

  /** ??? coalesce не может быть выше InMemoryTableScan/InMemoryRelation */
  df
    .filter($"id" > 0)
    .explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter ('id > 0)
    +- LogicalRDD [id#0L], false

    == Analyzed Logical Plan ==
    id: bigint
    Filter (id#0L > cast(0 as bigint))
    +- LogicalRDD [id#0L], false

    == Optimized Logical Plan ==
    Filter (id#0L > 0)
    +- InMemoryRelation [id#0L], StorageLevel(disk, memory, deserialized, 1 replicas)
          +- *(1) Scan ExistingRDD[id#0L]

    == Physical Plan ==
    *(1) Filter (id#0L > 0)
    +- *(1) ColumnarToRow
       // чтение из кэша
       +- InMemoryTableScan [id#0L], [(id#0L > 0)]
             // запись в кэш
             +- InMemoryRelation [id#0L], StorageLevel(disk, memory, deserialized, 1 replicas)
                   +- *(1) Scan ExistingRDD[id#0L]
   */

  spark
    .sharedState
    .cacheManager
    .clearCache


  /** !!! Partial Caching с coalesce - не работает. */
  val testDf2: Dataset[lang.Long] = spark.range(0, 80, 1, 8)

  spark.time {
    val dataDf: DataFrame = testDf2.select(udf_sleep())

    dataDf.cache
    /** Идея - coalesce не может переместиться выше кэширования + для минимизации используемой памяти используем Partial Caching. */
    dataDf.show(1)

    val resDf: Dataset[Row] = dataDf.coalesce(1)
    resDf.prettyExplain()
    resDf.explain
    /*
      == Physical Plan ==
      Coalesce 1
      +- InMemoryTableScan [UDF()#270]
            +- InMemoryRelation [UDF()#270], StorageLevel(disk, memory, deserialized, 1 replicas)
                  +- *(1) Project [UDF() AS UDF()#270]
                     +- *(1) Range (0, 80, step=1, splits=8)
     */
    resDf.collect

    println(resDf.rdd.getNumPartitions)
    /**
     * !!! 80,
     * а рассчитывали на 20 - 10 на dataDf.show(1) - расчет 1-й пертиции + 10 на resDf.collect - расчет оставшихся 7,
     * но этого не происходит
     */
  } // 80248 ms


  Thread.sleep(10000000)
}
