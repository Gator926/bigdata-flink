package org.myorg.quickstart

import org.apache.flink.api.java.utils.ParameterTool

object ParametersApp {
  def main(args: Array[String]): Unit = {
//    testFromArgs(args)
    testFromPropertiesFIle()
  }

  def testFromArgs(args: Array[String]): Unit ={
    val parameters = ParameterTool.fromArgs(args)
    val groupId = parameters.get("group.id", "test")
    val topics = parameters.getRequired("topics")
    println(groupId + "..." + topics)
  }

  def testFromPropertiesFIle(): Unit ={
    val parameters = ParameterTool.fromPropertiesFile("conf/bigdata-flink.properties")
    val groupId = parameters.get("group.id", "test")
    val topics = parameters.getRequired("topics")
    println(groupId + "..." + topics)
  }
}
