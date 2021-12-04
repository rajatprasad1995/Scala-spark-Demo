package com.scalaDemo.Tasks


import com.scalaDemo.utils.Readdata.{fetchCSV, fetchTable}
import com.scalaDemo.utils.WriteData.saveToCsv
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, coalesce, desc, rank, regexp_replace, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.nio.file.Paths


class CommomPesticideByState(spark: SparkSession, conf: (String, String, String, String, String)) {

  val log = Logger.getLogger(getClass.getName)

  import spark.implicits._

  def calculateMostCommonByState(): Unit = {
    log.info("Calculating the most common pesticide used by each state in the United  States")

    // setting path to read data

    val pathDatabase = Paths.get(conf._1, conf._2, conf._4)
    val pathCsvPestCode = Paths.get(conf._1, conf._3, "pest_codes.csv")

    // reading the sqlite database
    val preparedSampleDataDF = getPreparedSampledData(pathDatabase.toString)

    val filteredResultDataDF = getResultDataDF(pathDatabase.toString)

    // setting the join condition
    val joinCondition = filteredResultDataDF.col("sample_pk") === preparedSampleDataDF.col("sample_pk")

    // broadcasting the table containg sample data because it has fewer rows
    val joinedData = filteredResultDataDF.join(broadcast(preparedSampleDataDF), joinCondition, "inner")
      .select($"pestcode", $"origin state").persist()


    // calculating rank based on frequency with which each pesticide was used in a particular state
    val rankedDF = calculateRank(joinedData).withColumnRenamed("pestcode","Pest Code 1")

    // reading name of csv file containing names of each pesticide, first setting the schema of the file

    val pestCodeSchema = StructType(Array(
      StructField("Pest Code",StringType,false),
      StructField("Pesticide Name",StringType,false)
    ))

    val pesticideDataDF = fetchCSV(spark, pestCodeSchema, pathCsvPestCode.toString, delimiter = ",", header = true)
      .withColumn("Pest Code",regexp_replace($"Pest Code", "\\s+",""))

    //joining the ranked df and pesticide df

    val joinCondition2 = rankedDF.col("Pest Code 1") === pesticideDataDF.col("Pest Code")

    //broadcasting to improve performace
    val finalData = rankedDF.join(broadcast(pesticideDataDF), joinCondition2, "inner")
      .drop(rankedDF.col("Pest Code 1"))
      .select($"origin state",$"Pesticide Name",$"rank",$"count by origin and pest code",$"count by origin",$"% of total samples")


    //setting path to write data
    val outputPath = Paths.get(conf._5, "task2")

    saveToCsv(spark, finalData.sort($"origin state"),outputPath.toString )


  }

  def getPreparedSampledData(database: String): DataFrame = {
    val sampleDataDF = fetchTable(spark, "sampledata15", "", database)
      .select($"sample_pk", $"origin", $"growst", $"packst", $"distst")


    val filteredSampleDataDF = sampleDataDF.filter($"origin" === "1")

    val coalesceSampleDataDF = filteredSampleDataDF
      .withColumn("growst", regexp_replace($"growst", "\\s+", ""))
      .withColumn("packst", regexp_replace($"packst", "\\s+", ""))
      .withColumn("distst", regexp_replace($"distst", "\\s+", ""))
      .withColumn("growst", when($"growst" === "", null).otherwise($"growst"))
      .withColumn("packst", when($"packst" === "", null).otherwise($"packst"))
      .withColumn("distst", when($"distst" === "", null).otherwise($"distst"))
      .withColumn("origin state", coalesce($"growst", $"packst", $"distst"))
      .withColumn("origin state", regexp_replace($"origin state", "\\s+", ""))
      .where($"origin state".isNotNull)
      .filter($"origin state" =!= "")

    coalesceSampleDataDF.select($"sample_pk", $"origin state")

  }

  def getResultDataDF(database: String): DataFrame = {
    val resultDataDF = fetchTable(spark, "resultsdata15", "", database.toString).select($"pestcode", $"sample_pk")


    val filteredResultDataDF = resultDataDF.withColumn("pestcode", regexp_replace($"pestcode", "\\s+", ""))
      .withColumn("sample_pk", regexp_replace($"sample_pk", "\\s+", ""))
      .where($"pestcode".isNotNull)
      .filter($"pestcode" =!= "")
      .where($"sample_pk".isNotNull)
      .filter($"sample_pk" =!= "")
    filteredResultDataDF

  }

  def calculateRank(joinedData: DataFrame): DataFrame = {
    val countByState = joinedData.groupBy($"origin state").count()
      .withColumnRenamed("count","count by origin" )


    val countByPestAndState = joinedData.groupBy($"origin state", $"pestcode").count()
      .withColumnRenamed("count", "count by origin and pest code" )


    joinedData.unpersist()

    val windowSpec = Window.partitionBy("origin state")
      .orderBy(desc("count by origin and pest code"))


    val countByPestAndStateRanked = countByPestAndState.withColumn("rank", rank().over(windowSpec))
      .where($"rank" === 1)
      .withColumnRenamed("origin state","origin state 1")

    val joinCondition = countByPestAndStateRanked.col("origin state 1") === countByState.col("origin state")


    countByPestAndStateRanked.join(broadcast(countByState), joinCondition, "inner")
      .withColumn("% of total samples", ($"count by origin and pest code" / $"count by origin")*100)
      .drop("origin state 1")

  }

}
