package com.scalaDemo.loadConfig

import org.junit.runner.RunWith
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class LoadConfigTest extends FunSpec with Matchers{

  describe("Loading configurations") {

    it("testing a loaded configuration") {
      val conf = LoadConfig.returnConfig()
      assert(conf._1 === "test-data")

    }
  }

}
