package com.scalaDemo.driver

import org.junit.runner.RunWith
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class DriverTest  extends FunSpec with Matchers{

  describe("A Spark Driver") {

    it("should return spark session") {
      val spark = new Driver("Scala Spark test").returnSparkSession()
      assert(spark.sparkContext.getConf.get("spark.app.name") === "Scala Spark test")

    }
  }

}