package com.scalaDemo

import com.scalaDemo.loadConfig.LoadConfig
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.nio.file.Paths

package object consts {
  // setting path to import data, write output
  val conf   =LoadConfig.returnConfig()
  val pathDatabase = Paths.get(conf._1, conf._2, conf._4).toString
  val pathCsvPestCode = Paths.get(conf._1, conf._3, "pest_codes.csv").toString
  val pathCsvStateCode = Paths.get(conf._1, conf._3, "state_codes.csv").toString

  // setting schema of csv file
  val pestCodeSchema = StructType(Array(
    StructField("Pest Code",StringType,false),
    StructField("Pesticide Name",StringType,false)
  ))

  val StateCodeSchema = StructType(Array(
    StructField("State Code",StringType,false),
    StructField("State",StringType,false)
  ))

}
