package l_6

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.hybrid.{GetAirport, TimeFunctions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import shared.SparkCommons

class GetAirportsSpec extends AnyFlatSpec with should.Matchers with SparkCommons{
  import spark.implicits._

  /** test 1 */
//  println(GetAirport.getAirport("SSWB"))
//
//  spark
//    .range(1)
//    .select(lit("SSWB").as("value"))
//    .select(GetAirport.getAirportUdf($"value"))
//    .show()
//
//  println()

  /** test 2 */
//  println(GetAirport.getAirportRaw("SSWB"))
//
//  val nativeUdfDf: DataFrame =
//    spark
//      .range(1)
//      .select(lit("SSWB").as("value"))
//      .select(GetAirport.getAirportNativeUdf($"value", List("ident", "name", "iso_country", "type")))
//
//  nativeUdfDf.show(10, truncate = false)
//  nativeUdfDf.printSchema()
//  println()

  /** test 3 */
//  val nativeUdfDf2: DataFrame =
//    spark
//      .range(0, 100, 1, 1)
//      .select(lit("SSWB").as("value"))
//      .select(GetAirport.getAirportNativeUdf($"value", List("ident", "name", "iso_country", "type")))
//
//  nativeUdfDf2.collect()
//  println()
  /** !!! Конструктор кейс класса и методы dataType/inputTypes вызываются многократно => должны быть легковесными. */

  /** test 4 */
  val nativeUdfDf3: DataFrame =
    spark
      .range(0, 1, 1, 1)
      .select(current_timestamp().as("value"))
      .select(
        $"value",
        TimeFunctions.addHours($"value", 3)
      )

  nativeUdfDf3.show(10, truncate = false)
  nativeUdfDf3.printSchema()

}
