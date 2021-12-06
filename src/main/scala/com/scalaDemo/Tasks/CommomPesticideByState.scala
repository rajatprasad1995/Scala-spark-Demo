package com.scalaDemo.Tasks


import com.scalaDemo.utils.PrepareData.{getPreparedResultDataDF, getPreparedSampledData}
import com.scalaDemo.utils.ReadData.{fetchCSV, fetchTable}
import com.scalaDemo.utils.WriteData.saveToCsv
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, coalesce, col, desc, rank, regexp_replace, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.scalaDemo.consts._

import java.nio.file.Paths


class CommomPesticideByState(spark: SparkSession) {

  val log = Logger.getLogger(getClass.getName)

  import spark.implicits._

  def calculateMostCommonByState(): Unit = {
    log.info("Calculating the most common pesticide used by each state in the United  States")

    // reading the sqlite database
    val preparedSampleDataDF = getPreparedSampledData(spark, pathDatabase, List("sample_pk", "origin state"))

    val filteredResultDataDF = getPreparedResultDataDF(spark, pathDatabase, List("pestcode", "sample_pk"))

    // setting the join condition
    val joinCondition = filteredResultDataDF.col("sample_pk") === preparedSampleDataDF.col("sample_pk")


    val joinedData = filteredResultDataDF.join(preparedSampleDataDF, joinCondition, "inner")
      .select($"pestcode", $"origin state").persist()


    // calculating rank based on frequency with which each pesticide was used in a particular state
    val rankedDF = calculateRank(joinedData).withColumnRenamed("pestcode","Pest Code 1")

    // reading name of csv file containing names of each pesticide, first setting the schema of the file



    val pesticideDataDF = fetchCSV(spark, pestCodeSchema, pathCsvPestCode, delimiter = ",", header = true)
      .withColumn("Pest Code",regexp_replace($"Pest Code", "\\s+",""))

    //joining the ranked df and pesticide df

    val joinCondition2 = rankedDF.col("Pest Code 1") === pesticideDataDF.col("Pest Code")

    //broadcasting to improve performace
    val finalData = rankedDF.join(broadcast(pesticideDataDF), joinCondition2, "inner")
      .drop(rankedDF.col("Pest Code 1"))
      .select($"origin state",$"Pesticide Name",$"PEST CODE",$"rank",$"count by origin and pest code",$"count by origin",$"% of total samples")


    //setting path to write data
    val outputPath = Paths.get(conf._5, "task2")

    saveToCsv(spark, finalData.sort($"origin state"),outputPath.toString )


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
