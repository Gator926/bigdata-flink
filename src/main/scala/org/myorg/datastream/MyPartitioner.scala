package org.myorg.datastream

import org.apache.flink.api.common.functions.Partitioner

class MyPartitioner extends Partitioner[String]{
  override def partition(k: String, i: Int): Int = {
    println("numPartitions......." + i)
    if ("www.baidu.com" == k){
      1
    } else if ("www.qq.com" == k){
      2
    } else{
      0
    }
  }
}
