package com.scalaDemo

import com.scalaDemo.Tasks.MostCommonPesticide
import com.scalaDemo.driver.Driver
import com.scalaDemo.loadConfig.LoadConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object App {

  def main(args : Array[String]) {
    val conf   =LoadConfig.returnConfig()
    val spark = new Driver("Scala Spark Demo").returnSparkSession()
    val app = new App(spark, conf)
    app.run(args(0))
    spark.stop()
  }

}


class App(spark: SparkSession, conf:(String, String, String, String, String)){
  val log = Logger.getLogger(getClass.getName)
  def run(task:String) {

    log.info("empezando los jobs de ETL")

    task match {

      case "task1" => {

        val task1 = new MostCommonPesticide(spark, conf)
        task1.calculateMostCommon()

      }


      case _ => {

        log.error("se ha equivocado con el argumento")

      }
    }



  }
}


