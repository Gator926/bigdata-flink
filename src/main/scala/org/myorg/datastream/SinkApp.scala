package org.myorg.datastream

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object SinkApp {
  def main(args: Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value = env.readTextFile("data/access.log").map(x=>{
      val splits = x.split(",")
      (splits(1).trim, splits(2).trim.toLong)
    }).keyBy(_._1).sum(1)

    class MyRedisMapper extends RedisMapper[(String, Long)] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "bigdata-traffics")
      }

      override def getKeyFromData(t: (String, Long)): String = t._1

      override def getValueFromData(t: (String, Long)): String = t._2 + ""

      val conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.1.141").build()
      value.addSink(new RedisSink[(String, Long)](conf, new MyRedisMapper))
      env.execute()
    }
  }
}
