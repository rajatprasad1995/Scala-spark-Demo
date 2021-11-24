package com.scalaDemo.Tasks

import com.scalaDemo.utils.Readdata.fetchTable
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.log4j.Logger
import java.nio.file.Paths

class Playground(spark:SparkSession, conf:(String, String, String, String, String)) {
  val log = Logger.getLogger(getClass.getName)
  import spark.implicits._

  def showTables() = {

    val path = Paths.get(conf._1, conf._2, conf._4)
    val exampleDF = fetchTable(spark, "resultsdata15", "", path.toString)
    exampleDF.show(50)
  }

}
