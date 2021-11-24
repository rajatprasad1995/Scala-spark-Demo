package com.scalaDemo.loadConfig

import com.typesafe.config.ConfigFactory

object LoadConfig {

  def returnConfig()={

    val defaultConfig = ConfigFactory.load()

    val rootpathInput = defaultConfig.getString("data.input.main.path")

    val dbPath = defaultConfig.getString("db.path")
    val csvPath = defaultConfig.getString("csv.path")

    val dbName = defaultConfig.getString("db.name")

    val rootpathOutput = defaultConfig.getString("output.rootpath")

    val tupConf = (rootpathInput, dbPath, csvPath, dbName, rootpathOutput)
    tupConf

  }

}
