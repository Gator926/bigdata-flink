package org.myorg.datastream

import java.sql.{Connection, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.myorg.utils.MySQLUtils

object SinkApp {
  def main(args: Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value = env.readTextFile("data/access.log").map(x=>{
      val splits = x.split(",")
      (splits(1).trim, splits(2).trim.toLong)
    }).keyBy(_._1).sum(1)

//    class MyRedisMapper extends RedisMapper[(String, Long)] {
//      override def getCommandDescription: RedisCommandDescription = {
//        new RedisCommandDescription(RedisCommand.HSET, "bigdata-traffics")
//      }
//
//      override def getKeyFromData(t: (String, Long)): String = t._1
//
//      override def getValueFromData(t: (String, Long)): String = t._2 + ""
//
//
//    }
//    val conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.1.141").build()
//    value.addSink(new RedisSink[(String, Long)](conf, new MyRedisMapper))

    class MySQLSink extends RichSinkFunction[(String, Long)]{
      var connection: Connection = _
      var insertPsmt: PreparedStatement = _
      var updatePsmt: PreparedStatement = _
      override def open(parameters: Configuration): Unit = {
        connection = MySQLUtils.getConnection()
        insertPsmt = connection.prepareStatement("insert into traffic(domain , traffics) values(?,?) ")
        updatePsmt = connection.prepareStatement("update traffic set traffics = ? where domain = ? ")
      }

      override def close(): Unit = {
        insertPsmt.close()
        updatePsmt.close()
        connection.close()
      }

      override def invoke(value: (String, Long)): Unit = {
        updatePsmt.setLong(1, value._2)
        updatePsmt.setString(2 , value._1)
        updatePsmt.execute()

        if (updatePsmt.getUpdateCount == 0){
          insertPsmt.setString(1, value._1)
          insertPsmt.setLong(2, value._2)
          insertPsmt.execute()
        }
      }
    }
    value.addSink(new MySQLSink)
    env.execute()
  }
}
