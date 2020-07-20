package org.myorg.datastream

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object SourceApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // fromCollection 是单并行度的
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

    env.execute("Flink Streaming Scala API Skeleton")
  }
}
