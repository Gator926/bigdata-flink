package org.myorg.datastream

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.NumberSequenceIterator

object SourceApp001 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    fromSocket(env)
//    fromElements(env)
//    fromParallelCollection(env)
    fromTextFile(env)
    env.execute("Flink Streaming Scala API Skeleton")
  }

  // socket也是单并行度的
  def fromSocket(env: StreamExecutionEnvironment): Unit ={
    val stream = env.socketTextStream("localhost", 9999)
    println(stream.parallelism)     // 1
    val filterStream = stream.flatMap(_.split(",")).filter(_.trim() != "b")
    println(filterStream.parallelism)  // 8
    filterStream.print()
  }

  def fromCollection(env: StreamExecutionEnvironment): Unit ={
    val stream = env.fromCollection(List(
      Access(202010080110L, "www.baidu.com", 2000),
      Access(202010080110L, "www.qq.com", 2000),
      Access(202010080110L, "www.baidu.com", 3000),
      Access(202010080110L, "www.tao.com", 2000),
      Access(202010080110L, "www.baidu.com", 5000),
      Access(202010080110L, "www.tao.com", 2000)
    ))
    println(stream.parallelism)   // 1
    val filterStream = stream.filter(_.traffics > 4000)
    println(filterStream.parallelism)   // 8
    filterStream.print()
  }

  def fromElements(env: StreamExecutionEnvironment): Unit ={
    val stream = env.fromElements(1, "2", 3L, 4D, 5F)
    println(stream.parallelism)     // 1

    val mapStream = stream.map(x => x)
    println(mapStream.parallelism)    // 8

  }

  def fromParallelCollection(env: StreamExecutionEnvironment): Unit ={
    // 多并行度
    val stream = env.fromParallelCollection(new NumberSequenceIterator(1, 10))
    println(stream.parallelism)       // 8

    val filterStream = stream.filter(_ > 5)
    println(filterStream.parallelism)   // 8
    filterStream.print()
  }

  def fromTextFile(env: StreamExecutionEnvironment): Unit ={
    // 多并行度
    val stream = env.readTextFile("data/wc.txt")
    println(stream.parallelism)   // 8

    val mapStream = stream.map(x => x + ".bigdata.com")
    println(mapStream.parallelism)  // 8
    mapStream.print()

    val mapStream1 = stream.map(x => x + ".bigdata.com").setParallelism(2)
    println(mapStream1.parallelism)
    mapStream1.print()
  }
}
