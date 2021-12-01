package com.scalaDemo

import com.scalaDemo.Tasks.Playground
import com.scalaDemo.driver.Driver
import com.scalaDemo.loadConfig.LoadConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession



/**
 * @author ${user.name}
 */
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
      case "playground" => {

        val ex = new Playground(spark, conf)
        ex.showTables()
      }

      case "task1" => {

        val task1 = new AveragePurchaseCustomer(spark, conf)
        task1.calculateAverage()



      }
      case "task2" => {

        val task2 = new AverageMonthlySales(spark, conf)
        task2.calculateAverage()

      }
      case "task3" => {

        val task3 = new AverageSalesProduct(spark, conf)
        task3.calculateAverage()

      }

      case _ => {

        log.error("se ha equivocado con el argumento")

      }
    }



  }
}


