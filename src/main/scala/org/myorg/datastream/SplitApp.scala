package org.myorg.datastream

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object SplitApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("data/split.txt")
      .map(x=>{
        val splits = x.split(",")
        SplitAccess(splits(0).trim, splits(1).trim, splits(2).trim.toLong)
      })

    val guangdong = new OutputTag[SplitAccess]("广东省")
    val jiangsu = new OutputTag[SplitAccess]("江苏省")

    val splitStream = stream.process(new ProcessFunction[SplitAccess, SplitAccess] {
      override def processElement(value: SplitAccess, ctx: ProcessFunction[SplitAccess, SplitAccess]#Context, out: Collector[SplitAccess]): Unit = {
        if ("广东省" == value.province) {
          ctx.output(guangdong, value)
        } else if ("江苏省" == value.province) {
          ctx.output(jiangsu, value)
        }
      }
    })

    val guangdongSplits = splitStream.getSideOutput(guangdong)//.print("广东省分流结果")
//    splitStream.getSideOutput(jiangsu).print("江苏省分流结果")

    val shenzhen = new OutputTag[SplitAccess]("深圳市")
    val guangzhou = new OutputTag[SplitAccess]("广州市")

    val citySplits = guangdongSplits.process(new ProcessFunction[SplitAccess, SplitAccess] {
      override def processElement(value: SplitAccess, ctx: ProcessFunction[SplitAccess, SplitAccess]#Context, out: Collector[SplitAccess]): Unit = {
        if(value.city == "深圳市"){
          ctx.output(shenzhen, value)
        } else if (value.city == "广州市"){
          ctx.output(guangzhou, value)
        }
      }
    })

    citySplits.getSideOutput(shenzhen).print("深圳分流结果")
    citySplits.getSideOutput(guangzhou).print("广州分流结果")

    env.execute("Flink Streaming Scala API Skeleton")
  }

  def splits(env:StreamExecutionEnvironment): Unit ={
    val splitStream = env.readTextFile("data/split.txt")
      .map(x=>{
        val splits = x.split(",")
        SplitAccess(splits(0).trim, splits(1).trim, splits(2).trim.toLong)
      }).split(new OutputSelector[SplitAccess] {
      override def select(value: SplitAccess): lang.Iterable[String] = {
        val splits = new util.ArrayList[String]()
        if(value.province == "广东省"){
          splits.add("广东省")
        } else if (value.province == "江苏省"){
          splits.add("江苏省")
        }
        splits
      }
    })

    val guangdong = splitStream.select("广东省").print("广东省分流结果")
    val jiangsu = splitStream.select("江苏省").print("江苏省分流结果")
  }
}
