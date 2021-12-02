package l_1

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, udf}
import shared.SparkCommons

import java.lang

object AntiPatterns_3 extends SparkCommons with App {

  /**
   * 7. Передача null в Scala UDF.
   * Если в один из аргументов Scala UDF передается null, то функция может вернуть null без фактического вызова
   * т.е. тело даже не начнет выполняться.
   */
  def simpleFunc[T](x: T) = 1

  val int_udf: UserDefinedFunction = udf { simpleFunc[Int] _ }
  val long_udf: UserDefinedFunction = udf { simpleFunc[Long] _ }
  val string_udf: UserDefinedFunction = udf { simpleFunc[String] _ }

  val df: Dataset[lang.Long] = spark.range(1)

  val resDf1: DataFrame =
    df
      .select(
        int_udf(lit(1)),
        long_udf(lit(1L)),
        string_udf(lit("1"))
      )

  resDf1.show

  val resDf2: DataFrame =
    df
      .select(
        int_udf(lit(null)),
        long_udf(lit(null)),
        string_udf(lit(null))
      )

  resDf2.show
  /** В Scala string == java.long.string, а Long/Int - это собственные типы, которые не могут быть null. */
  /*
    +---------+---------+---------+
    |UDF(NULL)|UDF(NULL)|UDF(NULL)|
    +---------+---------+---------+
    |     null|     null|        1|
    +---------+---------+---------+
   */

  /** Решение - использовать типы java. */

//  2-23-00



  /**
   * Использование spark.locality.wait.
   *
   * spark.locality.wait - создана для обеспечения data locality (для уменьшение сетевого трфика)
   * т.е. обработка данных там, где они появились.
   *
   * Т.е. после репартиционирования они должны остаться в тех же jvm (locality level == process local),
   * если не получилось - будем пытаться обработать их на той же ноде (node local)
   * если не получилось - передача по сети.
   *
   * Проблематика: малое количество партиций в датафрейме из кафки => репартиционирование, но производительность не увеличилаcь.
   * Все партиции были распределены по воркерам, выполнившим начальное чтение из кафки (перекос по воркерам).
   *
   * Диагностика проблемы - cache; count - и посмотреть равномерно ли распределены закэшированные партиции по воркерам.
   *
   * Решение: spark.locality.wait = 0
   */

  /**
   * Запуск приложения с командами screen/nohup.
   * Зачем? Использовать deploy.mode = сluster.
   */

  /**
   * Чрезмерная нагрузка на DNS.
   *
   * Решение 1: отказаться от от dns и работать с ip-адресами. Неудобно.
   * Решение 2: прописать адреса хостов в etc.hosts. Но это решит проблему только с прямыми записями, с обратными - нет.
   * Решение 3: установка локального кэширующего dns на каждый узел кластера - который слушает только localhost.
   * + Перенаправление сервисов hadoop/spark/etc в dns local хоста.
   */

  /** Писать тесты) */


}
