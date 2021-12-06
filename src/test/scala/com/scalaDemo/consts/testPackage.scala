package com.scalaDemo.consts


import com.scalaDemo.driver.Driver
import com.scalaDemo.loadConfig.LoadConfig
import org.apache.spark.sql.SparkSession

import scala.collection._
import org.scalatest.{Assertions, FunSpec, Matchers}
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.scalaDemo.consts._
import java.nio.file.Paths

@RunWith(classOf[JUnitRunner])
class testPackage extends FunSpec with Matchers{
  val log = Logger.getLogger(getClass.getName)
  log.info("empezando los tests")

  describe("testing consts") {
    it("should return path of database") {

      val separator = System.getProperty("file.separator")
      assert(pathDatabase === s"test-data${separator}database${separator}test-database.sqlite")
    }

    it("should return path of pesticide csv files") {

      val separator = System.getProperty("file.separator")
      assert(pathCsvPestCode === s"test-data${separator}csvFiles${separator}pest_codes.csv")
    }

  }

}
