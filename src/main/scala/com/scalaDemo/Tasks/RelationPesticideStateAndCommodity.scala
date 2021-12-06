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

class RelationPesticideStateAndCommodity(spark: SparkSession) {

  //task 3
  val log = Logger.getLogger(getClass.getName)

  import spark.implicits._

  def investigateRelation():Unit={
    log.info("Investigating whether use of a particular pesticide depends on location or a commodity")

    // reading prepared sampledData15 table
    val preparedSampleDataDF = getPreparedSampledData(spark,
      pathDatabase, List("sample_pk", "origin state"))

    val preparedResultDataDF = getPreparedResultDataDF(spark, pathDatabase,
      List("sample_pk", "commtype", "pestcode"))

    val pesticideDataDF = fetchCSV(spark, pestCodeSchema, pathCsvPestCode, delimiter = ",", header = true)
      .withColumn("Pest Code",regexp_replace($"Pest Code", "\\s+",""))

    val stateDataDF = fetchCSV(spark, StateCodeSchema, pathCsvStateCode, delimiter = ",", header = true)
      .withColumn("State",regexp_replace($"State", "\\s+",""))
      .withColumn("State Code",regexp_replace($"State Code", "\\s+",""))

    val joinConditionPest = preparedResultDataDF.col("pestcode") === pesticideDataDF.col("Pest Code")

    val joinConditionState = preparedSampleDataDF.col("origin state") === stateDataDF.col("State Code")


    val enrichedResultDataDF = preparedResultDataDF.join(broadcast(pesticideDataDF), joinConditionPest, "inner")

    val enrichedSampleDataDF = preparedSampleDataDF.join(broadcast(stateDataDF), joinConditionState, "inner")


    val joinCondition2 = enrichedSampleDataDF.col("sample_pk") ===  enrichedResultDataDF.col("sample_pk")


    val joinedData = enrichedSampleDataDF.join( enrichedResultDataDF, joinCondition2, "inner")
      .select( $"commtype",$"State", $"Pesticide Name").persist()

    val groupedByStateAndPest = joinedData.groupBy($"State", $"Pesticide Name").count()
    val groupedByCommodityAndPest = joinedData.groupBy($"commtype", $"Pesticide Name").count()


    val finalDFCommodityPest = groupedByCommodityAndPest.groupBy( $"Pesticide Name")
      .pivot($"commtype")
      .sum("count")
      .na.fill(0)


    val finalDFStatePest = groupedByStateAndPest.groupBy( $"Pesticide Name")
      .pivot($"State")
      .sum("count")
      .na.fill(0)

    val outputPathCommodity = Paths.get(conf._5, "task3-Commodity-Pest")
    val outputPathState = Paths.get(conf._5, "task3-State-Pest")

    saveToCsv(spark, finalDFCommodityPest.sort($"Pesticide Name"),outputPathCommodity.toString )

    saveToCsv(spark, finalDFStatePest.sort($"Pesticide Name"),outputPathState.toString )


  }


}
