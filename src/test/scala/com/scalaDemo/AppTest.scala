package com.scalaDemo

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
class AppTest extends FunSpec with Matchers {

  val log = Logger.getLogger(getClass.getName)
  log.info("empezando los tests")

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Scala Spark test")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .getOrCreate()

  describe("testing task one") {
    it("counting for a pest code") {
      import spark.implicits._
      val task1 = new App(spark)
      task1.run("task1")

      val resultSchema = StructType(Array(
        StructField("Pest Code", StringType, false),
        StructField("Test Class", StringType, false),
        StructField("count", IntegerType, false),
        StructField("Pesticide Name", StringType, false)
      ))


      val task1DF = spark.read
        .option("header", true)
        .schema(resultSchema)
        .csv(s"${Paths.get(conf._5, "task1").toString}.csv")

      task1DF.show(10)
      val valueToCheck = task1DF.filter($"Pest Code" === "001").select($"count").collect()(0)(0)
      val valueToCheck2 = task1DF.filter($"Pest Code" === "002").select($"count").collect()(0)(0)

      assert(valueToCheck === 5)
      assert(valueToCheck2 === 2)
    }

  }
  describe("testing task two") {
    it("counting rows for a state") {
      import spark.implicits._
      val task1 = new App(spark)
      task1.run("task2")

      val resultSchema = StructType(Array(
        StructField("origin state", StringType, false),
        StructField("Pesticide Name", StringType, false),
        StructField("PEST CODE", StringType, false),
        StructField("rank", IntegerType, false)
      ))


      val task1DF = spark.read
        .option("header", true)
        .schema(resultSchema)
        .csv(s"${Paths.get(conf._5, "task2").toString}.csv")

      task1DF.show(10)
      val valueToCheck = task1DF.where($"origin state" === "WA" && $"PEST CODE" === "042")
        .select($"rank").collect()(0)(0)

      assert(valueToCheck === 1)
    }
  }
    describe("testing task three") {
      it("counting number of samples containing pesticide 'Diuron' in Alaska") {
        import spark.implicits._
        val task = new App(spark)
        task.run("task3")

        val resultPestSchema = StructType(Array(
          StructField("Pesticide Name", StringType, false),
          StructField("Alaska", IntegerType, false)
        ))


        val taskStatePestDF = spark.read
          .option("header", true)
          .schema(resultPestSchema)
          .csv(s"${Paths.get(conf._5, "task3-State-Pest").toString}.csv")


        val valueToCheck = taskStatePestDF.where($"Pesticide Name" === "Diuron")
          .select($"Alaska").collect()(0)(0)

        assert(valueToCheck === 2)
      }


    }

}