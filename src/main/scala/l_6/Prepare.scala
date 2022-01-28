package l_6

import org.apache.spark.sql.{DataFrame, SaveMode}
import shared.SparkCommons

/**
 * docker ps
 *
 * https://www.mongodb.com/compatibility/docker
 * docker run --name mongodb -d -p 27017:27017 mongo - 1й запуск контейнера
 * docker start mongodb - 2-й и последующие запуски контейнера
 * docker exec -it 787a7f6ea3ce mongo
 *
 * use test
 * db.file_index.findOne()
 * db.file_index.find()
 * db.file_index.deleteMany( { "objectName" : "test01"} )
 *
 * docker stop mongodb
 */

/**!!! Нет коннектора к монге под 2.13. */
object Prepare extends SparkCommons with App {
  import spark.implicits._

  val airportCodes: DataFrame =
    spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/l_6/airport-codes_csv.csv")

  airportCodes
    .withColumn("_id", $"ident")
    .write
    .format("mongo")
    .option("uri", "mongodb://localhost:27017")
    .option("database", "test")
    .option("collection", "airports")
    .mode(SaveMode.Overwrite)
    .save()
}


