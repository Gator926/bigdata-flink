package org.myorg.quickstart

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SpecifyingKeysApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9000)

    text.flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map(WC(_,1))
      .keyBy(_.word)
      .sum("count")
      .print()

    env.execute(this.getClass.getSimpleName)
  }

  case class WC(word: String, count: Int)
}
