package org.myorg.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

object MySQLUtils {
  def getConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "root")
  }

  def close(connection:Connection, pstmt:PreparedStatement): Unit ={
    if (null != pstmt){
      pstmt.close()
    }
    if (null != connection){
      connection.close()
    }
  }
}
