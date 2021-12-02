package shared

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.{Dataset, SparkSession}

trait SparkCommons {
  Logger.getLogger("org")
    .setLevel(Level.ERROR)

  lazy val spark: sql.SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName("l_1")
    .getOrCreate

  lazy val sc: SparkContext = spark.sparkContext

  /**
   * Функция для отображения плана.
   * ??? Переделать под 3.2.0
   */
  implicit class PrintExplain(dataset: Dataset[_]) {
    def prettyExplain(): Unit = {
      val arrowDown: Char = '\u2B07'

      val prettyString: String =
        dataset
          .queryExecution
          .executedPlan
          .flatMap {
            case sp if "InputAdapter" :: "WholeStageCodegen" :: Nil contains sp.nodeName => None
            case sp => Some(sp)
          }
//          .map(_.simpleString)  // spark 2.4.8
          .map(_.simpleString(10))  // spark 3.2.0
          .reverse
          .mkString(s"\n$arrowDown\n")

      println(prettyString)
    }
  }

}
