package com.scalaDemo

import com.scalaDemo.driver.Driver

import scala.collection._
import org.scalatest.{Assertions, FunSpec, Matchers}
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class AppTest extends FunSpec with Matchers {

  describe("A Spark Driver") {

  it("should return spark session") {
  val spark = new Driver("Scala Spark test").returnSparkSession()
  assert(spark.sparkContext.getConf.get("spark.app.name") === "Scala Spark test")

}
}
}