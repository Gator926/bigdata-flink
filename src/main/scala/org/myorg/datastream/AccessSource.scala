package org.myorg.datastream

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class AccessSource extends SourceFunction[Access]{
  var running = true

  val random = new Random()

  var domains = Array("www.baidu.com", "www.qq.com", "www.google.com", "www.aliyun.com")

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
