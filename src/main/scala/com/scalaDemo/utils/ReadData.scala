package com.scalaDemo.utils

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object ReadData {

  def fetchTable(spark: SparkSession, tableName: String, schema: String, database: String): DataFrame = {
    val df = spark.read
      .format("jdbc")
      .option("url", s"jdbc:sqlite:${database}")
      .option("dbtable", tableName)
      .option("driver", "org.sqlite.JDBC")
      .load()
    df
  }

  def fetchCSV(spark: SparkSession, schema: StructType, path:String, delimiter: String, header:Boolean):DataFrame = {
    val df = spark.read.option("header", header)
      .option("delimiter",delimiter)
      .schema(schema)
      .csv(path)
    df
  }
}