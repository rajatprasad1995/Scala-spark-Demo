package com.scalaDemo.driver

import org.apache.spark.sql.SparkSession

class Driver(name:String) {
  // creating a spark driver

  def returnSparkSession(): SparkSession ={
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .enableHiveSupport()
      .appName(name)
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .getOrCreate()
    spark
  }

}
