package org.myorg.datastream

import org.apache.flink.api.common.functions.RichFilterFunction

class MyFilter(traffics:Long) extends RichFilterFunction[Access]{
  override def filter(t: Access): Boolean = t.traffics > traffics
}
