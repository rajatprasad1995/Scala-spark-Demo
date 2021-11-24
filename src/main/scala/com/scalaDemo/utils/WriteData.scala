package com.scalaDemo.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object WriteData {

  def saveToCsv(spark: SparkSession, outputDF: DataFrame,  path: String): Unit = {

    outputDF.repartition(1).write.mode(SaveMode.Overwrite).csv(s"${path}.csv")


  }

  def saveToParquet(spark: SparkSession, outputDF: DataFrame,  path: String): Unit = {

    outputDF.write.mode(SaveMode.Overwrite).parquet(s"${path}.parquet")
  }

}
