package l_1

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{from_json, get_json_object, lit, struct, to_json, udf}
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import shared.SparkCommons

import java.lang

object AntiPatterns_2 extends SparkCommons with App {
  import spark.implicits._

  /**
   * 4. Неявный повторный вызов Scala UDF.
   * Проявляется при работе с нескалярными данными - array/struct и дальнейшей работе с их элементами.
   * Если внутри проекции несколько раз присутствует вызов UDF, то под капотом она будет вызвана несколько раз на каждую строчку.
   */
  val udf_empty_list1: UserDefinedFunction = udf { () => { Thread.sleep(1000); List.empty[Int] } }

  val df: Dataset[lang.Long] = spark.range(0, 10, 1, 1)

//  spark.time {
//    val dataDf: DataFrame =
//      df
//        .select(udf_empty_list1().as("f"))
//        .select($"f"(0), $"f"(1))
//
//    dataDf.prettyExplain()
//    dataDf.explain(true)
    /*
      == Parsed Logical Plan ==
      'Project [unresolvedalias('f[0], Some(org.apache.spark.sql.Column$$Lambda$1312/0x0000000800b91840@186d6033)), unresolvedalias('f[1], Some(org.apache.spark.sql.Column$$Lambda$1312/0x0000000800b91840@186d6033))]
      +- Project [UDF() AS f#3]
         +- Range (0, 10, step=1, splits=Some(1))

      == Analyzed Logical Plan ==
      f[0]: int, f[1]: int
      Project [f#3[0] AS f[0]#5, f#3[1] AS f[1]#6]
      +- Project [UDF() AS f#3]
         +- Range (0, 10, step=1, splits=Some(1))

      == Optimized Logical Plan ==
      Project [UDF()[0] AS f[0]#5, UDF()[1] AS f[1]#6]
      +- Range (0, 10, step=1, splits=Some(1))

      == Physical Plan ==
      *(1) Project [UDF()[0] AS f[0]#5, UDF()[1] AS f[1]#6]
      +- *(1) Range (0, 10, step=1, splits=1)
     */
//    dataDf.collect
    /** ??? Должно быть 20 секунд - Spark 2.4.8 */
    /** Холодный старт дает + ~1 секунду к времени работы первого блока в программе. */
//  } // 10247 ms

  /** Решение 1 - asNondeterministic. Может рабоать не всегда. */
  val udf_empty_list2: UserDefinedFunction = udf { () => { Thread.sleep(1000); List.empty[Int]} }.asNondeterministic

//  spark.time {
//    val dataDf: DataFrame =
//      df
//        .select(udf_empty_list2().as("f"))
//        .select($"f"(0), $"f"(1))
//
//    dataDf.prettyExplain()
//    dataDf.explain
    /*
      *(1) Project [f#4[0] AS f[0]#6, f#4[1] AS f[1]#7]
      +- *(1) Project [UDF() AS f#4]
         +- *(1) Range (0, 10, step=1, splits=1)
     */
//    dataDf.collect
//  } // 10106 ms

  /** Решение 2 - cache; count/repartition. */
  df.localCheckpoint

//  spark.time {
//    val tmpDf: DataFrame = df.select(udfEmptyList1().as("f"))
//    tmpDf.cache
//    tmpDf.count
//
//    val dataDf: DataFrame = tmpDf.select($"f"(0), $"f"(1))
//    dataDf.prettyExplain()
//    dataDf.explain
    /*
      == Physical Plan ==
      *(1) Project [f#7[0] AS f[0]#40, f#7[1] AS f[1]#41]
      +- InMemoryTableScan [f#7]
            +- InMemoryRelation [f#7], StorageLevel(disk, memory, deserialized, 1 replicas)
                  +- *(1) Project [UDF() AS f#7]
                     +- *(1) Range (0, 10, step=1, splits=1)
     */
//    dataDf.collect
//  } // 10631 ms


  /**
   * 5. Несоответствие типов при работе с JSON.
   * В случае небольших несоответствий типов между схемой и JSON документом - функция парсинга возвращает null на всю cnhernehe/
   */

  /**
   * 3_000_000_000 укладывается в unsigned integer => [0 to 4_294_967_295]
   * но в Spark signed int => [-2_147_483_648 до 2_147_483_647]
   */
  val arrowDown: Char = '\u2B07'
  val testJsonString: String = s"""{ "foo": "bar$arrowDown", "number": 3000000000, "baz": true }"""

  val schema1: StructType = StructType(List(
    StructField("foo", StringType),
    /**
     * Проблема появляется не только при конверсии Long => IntegerType,
     * но и для других типов String => LongType/String => BooleanType и др.
     *
     * Int => StringType - работает.
     */
    StructField("number", IntegerType),
    StructField("baz", BooleanType)
  ))

  val df2: DataFrame =
    spark
      .range(1)
      .select(from_json(lit(testJsonString), schema1).as("f"))
      .select($"f.*")

  df2.show(1)
  /*
    Spark 3.2.0
    +---+------+----+
    |foo|number| baz|
    +---+------+----+
    |bar|  null|true|
    +---+------+----+

    ??? Spark 2.4.8 должен вернуть - т.е. занулить все колонки.
    +----+------+----+
    |foo |number| baz|
    +----+------+----+
    |null|  null|null|
    +----+------+----+
   */
  /**
   * Решения для Spark_2.4:
   * 1. Переписать функцию парсинга from_json.
   * 2. Доставать корневые поля через get_json_object (достает элемент json как строку), а вложенные в них с помощью from_json.
   */

  /** Long вмещает 3_000_000_000 */
  val schema2: StructType = StructType(List(
    StructField("foo", StringType),
    StructField("number", LongType),
    StructField("baz", BooleanType)
  ))

  val df3: DataFrame =
    spark
      .range(1)
      .select(from_json(lit(testJsonString), schema2).as("f"))
      .select($"f.*")

  df3.show

  /** get_json_object */
  val rawDf: DataFrame =
    spark
      .range(1)
      .select(lit(testJsonString).as("f"))
      .select(
        get_json_object($"f", "$.foo"),
        get_json_object($"f", "$.number"),
        get_json_object($"f", "$.baz")
      )

  /**
   * Каждый вызов get_json_object - это парсинг json => дополнительный оверхэд (небольшой - парсинг json в Spark очень быстрый).
   * ??? Аналогично каждый раз вызывается и from_json (если нет cache; count).
   */
  rawDf.explain(true)
  /*
    == Parsed Logical Plan ==
    'Project [unresolvedalias(get_json_object('f, $.foo), Some(org.apache.spark.sql.Column$$Lambda$2425/0x0000000801038040@7da1ef46)), unresolvedalias(get_json_object('f, $.number), Some(org.apache.spark.sql.Column$$Lambda$2425/0x0000000801038040@7da1ef46)), unresolvedalias(get_json_object('f, $.baz), Some(org.apache.spark.sql.Column$$Lambda$2425/0x0000000801038040@7da1ef46))]
    +- Project [{ "foo": "bar", "number": 3000000000, "baz": true } AS f#55]
       +- Range (0, 1, step=1, splits=Some(8))

    == Analyzed Logical Plan ==
    get_json_object(f, $.foo): string, get_json_object(f, $.number): string, get_json_object(f, $.baz): string
    Project [get_json_object(f#55, $.foo) AS get_json_object(f, $.foo)#57, get_json_object(f#55, $.number) AS get_json_object(f, $.number)#58, get_json_object(f#55, $.baz) AS get_json_object(f, $.baz)#59]
    +- Project [{ "foo": "bar", "number": 3000000000, "baz": true } AS f#55]
       +- Range (0, 1, step=1, splits=Some(8))

    == Optimized Logical Plan ==
    Project [bar AS get_json_object(f, $.foo)#57, 3000000000 AS get_json_object(f, $.number)#58, true AS get_json_object(f, $.baz)#59]
    +- Range (0, 1, step=1, splits=Some(8))

    == Physical Plan ==
    *(1) Project [bar AS get_json_object(f, $.foo)#57, 3000000000 AS get_json_object(f, $.number)#58, true AS get_json_object(f, $.baz)#59]
    +- *(1) Range (0, 1, step=1, splits=8)
   */
  rawDf.show


  /**
   * 6. Использование Dataset API трансформаций.
   * Под капотом Dataset API могут возникать дополнительные физические операторы.
   */

  val testDf: Dataset[lang.Long] =
    spark
      .range(1000)
      .localCheckpoint

  val resDf1: DataFrame = testDf.select($"id" + 1)
  resDf1.prettyExplain()
  resDf1.explain
  /*
    == Physical Plan ==
    *(1) Project [(id#76L + 1) AS (id + 1)#81L]
    +- *(1) Scan ExistingRDD[id#76L]
   */

  val testDs: Dataset[Long] =
    spark
      .range(1000)
      .as[Long]
      .localCheckpoint

  val resDs1: Dataset[Long] = testDs.map(x => x + 1)
  resDs1.prettyExplain()
  resDs1.explain
  /**
   * Под капотом датафрейма данные хранятся в виде org.apache.spark.sql.catalyst.InternalRow.
   * Технически InternalRow это Array[Any].
   *
   * .map(x => x + 1) - принимает и отдает Long, а не InternalRow - поэтому необходима цепочка трансформаций:
   * DeserializeToObject (распаковка InternalRow => Long) => MapElements (применение лямбды) => SerializeFromObject (запаковка Long => InternalRow)
   * !!! Оверхэд.
   */
  /*
    == Physical Plan ==
    *(1) SerializeFromObject [input[0, bigint, false] AS value#94L]
    +- *(1) MapElements l_1.AntiPatterns_2$$$Lambda$2562/0x00000008010ad040@6b30ff23, obj#93: bigint
       +- *(1) DeserializeToObject id#83: bigint, obj#92: bigint
          +- *(1) Scan ExistingRDD[id#83L]
   */

  val resDf2: Dataset[Long] = testDs.filter($"id" > 0)
  resDf2.prettyExplain()
  resDf2.explain
  /*
    == Physical Plan ==
    *(1) Filter (id#83L > 0)
    +- *(1) Scan ExistingRDD[id#83L]
   */

  val resDs2 = testDs.filter(x => x > 0)
  resDs2.prettyExplain()
  resDs2.explain
  /** filter на ds выполняется без оверхэда. */
  /*
    == Physical Plan ==
    *(1) Filter l_1.AntiPatterns_2$$$Lambda$2609/0x00000008010d5840@112a50a1.apply$mcZJ$sp
    +- *(1) Scan ExistingRDD[id#83L]
   */

  val resDf3: DataFrame = testDf.select(to_json(struct($"*")))
  resDf3.prettyExplain()
  resDf3.explain
  /*
    == Physical Plan ==
    Project [to_json(struct(id, id#76L), Some(Europe/Moscow)) AS to_json(struct(id))#101]
    +- *(1) Scan ExistingRDD[id#76L]
   */

  val resDs3: Dataset[String] = testDf.toJSON
  resDs3.prettyExplain()
  resDs3.explain
  /*
    == Physical Plan ==
    *(2) SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false) AS value#106]
    +- MapPartitions org.apache.spark.sql.Dataset$$Lambda$2625/0x00000008010e0840@62fbefad, obj#105: java.lang.String
       +- DeserializeToObject staticinvoke(class java.lang.Long, ObjectType(class java.lang.Long), valueOf, id#76L, true, false), obj#104: java.lang.Long
          +- *(1) Scan ExistingRDD[id#76L]
   */

}
