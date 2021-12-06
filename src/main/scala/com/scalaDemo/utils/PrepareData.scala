package com.scalaDemo.utils

import com.scalaDemo.utils.ReadData.fetchTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, regexp_replace, when}

object PrepareData {

  def getPreparedSampledData(spark:SparkSession, database: String, colSeq:List[String]): DataFrame = {
    val sampleDataDF = fetchTable(spark, "sampledata15", "", database)

    import spark.implicits._

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

    coalesceSampleDataDF.select(colSeq.map(col): _*)

  }


  def getPreparedResultDataDF(spark:SparkSession,database: String, listCol:List[String]): DataFrame = {
    import spark.implicits._
    val resultDataDF = fetchTable(spark, "resultsdata15", "", database).select(listCol.map(col) :_*)


    val filteredResultDataDF = resultDataDF.withColumn("pestcode", regexp_replace($"pestcode", "\\s+", ""))
      .withColumn("sample_pk", regexp_replace($"sample_pk", "\\s+", ""))
      .where($"pestcode".isNotNull)
      .filter($"pestcode" =!= "")
      .where($"sample_pk".isNotNull)
      .filter($"sample_pk" =!= "")
    filteredResultDataDF

  }

}
