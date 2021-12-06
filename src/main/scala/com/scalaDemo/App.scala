package com.scalaDemo

import com.scalaDemo.Tasks.{CommomPesticideByState, MostCommonPesticide, RelationPesticideStateAndCommodity}
import com.scalaDemo.driver.Driver
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object App {

  def main(args : Array[String]) {
    val spark = new Driver("Scala Spark Demo").returnSparkSession()
    val app = new App(spark)
    app.run(args(0))
    spark.stop()
  }

}


class App(spark: SparkSession){
  val log = Logger.getLogger(getClass.getName)
  def run(task:String) {

    log.info("empezando los jobs de ETL")

    task match {

      case "task1" => {

        val task1 = new MostCommonPesticide(spark)
        task1.calculateMostCommon()

      }

      case "task2" => {

        val task = new CommomPesticideByState(spark)
        task.calculateMostCommonByState()

      }


      case "task3" => {

        val task = new RelationPesticideStateAndCommodity(spark)
        task.investigateRelation()

      }

      case _ => {

        log.error("se ha equivocado con el argumento")

      }
    }

  }
}


