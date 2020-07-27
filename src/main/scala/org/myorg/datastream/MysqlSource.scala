package org.myorg.datastream

import java.sql.PreparedStatement

import java.sql.Connection
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.myorg.utils.MySQLUtils

class MysqlSource extends RichParallelSourceFunction[Student]{

  var connection:Connection = _
  var pstmt:PreparedStatement = _

  /**
   * 初始化操作
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    connection = MySQLUtils.getConnection()
    pstmt = connection.prepareStatement("select * from student")
  }

  override def close(): Unit = {
    MySQLUtils.close(connection, pstmt)
  }

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    val rs = pstmt.executeQuery()
    while (rs.next()){
      val id = rs.getInt("id")
      val name = rs.getString("name")
      val age = rs.getInt("age")
      ctx.collect(Student(id, name, age))
    }
  }

  override def cancel(): Unit = {

  }
}
