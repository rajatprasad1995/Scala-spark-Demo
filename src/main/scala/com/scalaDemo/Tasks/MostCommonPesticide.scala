package com.scalaDemo.Tasks

import com.scalaDemo.utils.Readdata.{fetchCSV, fetchTable}
import com.scalaDemo.utils.WriteData.{saveToCsv}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.nio.file.Paths

class MostCommonPesticide(spark:SparkSession, conf:(String, String, String, String, String)) {
  val log = Logger.getLogger(getClass.getName)
  import spark.implicits._

  def calculateMostCommon():Unit={
    log.info("Calculating how often a pesticide was found in the study task 1")
    // read data

    val pathDatabase = Paths.get(conf._1, conf._2, conf._4)

    val pathCsv = Paths.get(conf._1, conf._3, "pest_codes.csv")
    // reading the sqlite database
    val resultDataDF = fetchTable(spark, "resultsdata15", "", pathDatabase.toString).select($"pestcode", $"testclass")

    // reading the CSV file

    // setting up the schema

    val pestCodeSchema = StructType(Array(
      StructField("Pest Code",StringType,false),
      StructField("Pesticide Name",StringType,false),
      StructField("Test Class",StringType,false)
    ))

    val keyPesticideDataDF = fetchCSV(spark, pestCodeSchema, path= pathCsv.toString, delimiter = ",", header = true)
      .withColumn("Pest Code",regexp_replace($"Pest Code", "\\s+",""))



    // calculate stuff on resultDataDF
    val resultDataAgg = resultDataDF.withColumnRenamed("pestcode","Pest Code")
      .withColumnRenamed("testclass","Test Class")
      .groupBy($"Pest Code",$"Test Class").count()


    resultDataAgg.show()
    val joinCondition = resultDataAgg.col("Pest Code") === keyPesticideDataDF.col("Pest Code") && resultDataAgg.col("Test Class") === keyPesticideDataDF.col("Test Class")
    val joinedResultPestCode = resultDataAgg.join(keyPesticideDataDF, joinCondition,"left_outer")
      .select(resultDataAgg.col("Pest Code"),resultDataAgg.col("Test Class"),
        resultDataAgg.col("count"),keyPesticideDataDF.col("Pesticide Name"))

    // write results

    joinedResultPestCode.sort($"count".desc, $"Pesticide Name").show()

    val outputPath = Paths.get(conf._5, "task1")

    saveToCsv(spark, joinedResultPestCode.sort($"count".desc, $"Pesticide Name"),outputPath.toString )



  }

}
