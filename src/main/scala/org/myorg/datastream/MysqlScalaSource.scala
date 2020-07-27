package org.myorg.datastream


import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs


class MysqlScalaSource extends RichSourceFunction[Student]{

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    DBs.setupAll()

    DB.readOnly { implicit session => {
      SQL("select * from student").map(rs=>{
        val id = rs.int("id")
        val name = rs.string("name")
        val age = rs.int("age")
        ctx.collect(Student(id, name, age))
      }).list().apply()
    }}
  }

  override def cancel(): Unit = ???
}
