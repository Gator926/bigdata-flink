package org.myorg.datastream

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object SourceApp003 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val stream = env.addSource(new MysqlSource)
    val stream = env.addSource(new MysqlScalaSource)

    println(stream.parallelism)

    stream.print()

    env.execute("Flink Streaming Scala API Skeleton")
  }

}
