package org.myorg.datastream

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}

import scala.util.Random

class AccessSource003 extends RichParallelSourceFunction[Access]{
  var running = true

  val random = new Random()

  var domains = Array("www.baidu.com", "www.qq.com", "www.google.com", "www.aliyun.com")

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    println("open.invoked")
  }

  override def close(): Unit = {
    super.close()
    println("close.invoked")
  }

  override def run(sourceContext: SourceFunction.SourceContext[Access]): Unit = {
    while (running){
      val time = System.currentTimeMillis()
      1.to(10).foreach(x=>{
        sourceContext.collect(Access(time, domains(random.nextInt(domains.length)), random.nextInt(1000) + x))
      })
      Thread.sleep(5000)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
