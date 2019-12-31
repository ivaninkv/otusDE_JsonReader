package com.example

import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

object JsonReader extends App {
  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

  val spark = SparkSession.builder().master(master = "local").getOrCreate()
  val sc = spark.sparkContext
  val filename = args(0)
  //  val filename = "C:\\Users\\ivani\\Desktop\\sbt\\json_reader_ivaninkv\\src\\main\\scala\\com\\example\\winemag-data-130k-v2.json"
  val rdd = sc.textFile(filename)

  case class WineMag(id: Option[Int],
                     country: Option[String],
                     points: Option[Int],
                     price: Option[Double],
                     title: Option[String],
                     variety: Option[String],
                     winery: Option[String]
                    )

  println("===================================================")
  rdd.map(row =>
    parse(row).extract[WineMag]
  ).foreach(println)
  println("\n===================================================")
}
