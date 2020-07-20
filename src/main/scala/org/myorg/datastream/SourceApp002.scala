package org.myorg.datastream

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.NumberSequenceIterator

object SourceApp002 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val stream = env.addSource(new AccessSource)
//    println(stream.parallelism)     // 1
//    stream.print()

//    val stream = env.addSource(new AccessSource002)
//    println(stream.parallelism)     // 8
//    stream.print()

//    val stream = env.addSource(new AccessSource003)
//    println(stream.parallelism)     // 8
//    stream.print()

      val stream = env.addSource(new AccessSource003).setParallelism(3)
      println(stream.parallelism)     // 3
      stream.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }

}
