package org.myorg.datastream

import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object TransformationApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    partitionCustom(env)
    env.execute("Flink Streaming Scala API Skeleton")
  }

  def partitionCustom(environment: StreamExecutionEnvironment): Unit ={
    environment.addSource(new AccessSource)
      .map(x=>(x.domain, x))
      .partitionCustom(new MyPartitioner, 0)
      .map(x=>{
        println("------" + Thread.currentThread().getId)
        x._2
      }).print()
  }

  def connectUnion(environment: StreamExecutionEnvironment): Unit ={
    val contents = environment.readTextFile("data\\access.log")
    val accessStream = contents.map(x => {
      val splits = x.split(",")
      Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
    }).keyBy("domain").sum("traffics")

    val splitStream: SplitStream[Access] = accessStream.split(x => {
      if (x.traffics <= 3000) {
        Seq("非大客户啊")
      } else {
        Seq("大客户")
      }
    })

    val s1 = splitStream.select("大客户")
    val s2 = splitStream.select("非大客户")

    val connectedStream = s1.map(x => (x.time, x.traffics)).connect(s2)
    connectedStream.map(x=>{
      (x._1, x._2, "rz")
    }, y=>{
      (y.time, y.domain, y.traffics, "---")
    }).print()
  }

  def split(environment: StreamExecutionEnvironment): Unit ={
    val contents = environment.readTextFile("data\\access.log")
    val accessStream = contents.map(x => {
      val splits = x.split(",")
      Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
    }).keyBy("domain").sum("traffics")

    val splitStream: SplitStream[Access] = accessStream.split(x => {
      if (x.traffics <= 3000) {
        Seq("非大客户啊")
      } else {
        Seq("大客户")
      }
    })

    splitStream.select("大客户").print()
  }

  def keyBy(environment: StreamExecutionEnvironment): Unit ={
    val contents = environment.readTextFile("data\\access.log")
    val accessStream = contents.map(x => {
      val splits = x.split(",")
      Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
    })
//    accessStream.keyBy("domain").sum("traffics").print()
    accessStream.keyBy("domain").reduce((x,y)=>Access(x.time, x.domain, x.traffics+y.traffics)).print()
  }

  def filter(environment: StreamExecutionEnvironment): Unit ={
    val contents = environment.readTextFile("data\\access.log")
    val accessStream = contents.map(x => {
      val splits = x.split(",")
      Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
    })
    //    accessStream.filter(_.traffics > 2000).print()
    //
    //    accessStream.filter(new RichFilterFunction[Access] {
    //      override def filter(t: Access): Boolean = {
    //        t.traffics > 1000
    //      }
    //    })

    accessStream.filter(new MyFilter(4000)).print()
  }

  def map(environment: StreamExecutionEnvironment): Unit ={
    val contents = environment.readTextFile("data\\access.log")
    val accessStream = contents.map(x => {
      val splits = x.split(",")
      Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
    })

//    contents.map(new RichMapFunction[String, Access] {
//
//      override def open(parameters: Configuration): Unit = {
//        println("open")
//      }
//      override def map(in: String): Access = {
//        val splits = in.split(",")
//        Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
//      }
//    }).print()
  }
}
