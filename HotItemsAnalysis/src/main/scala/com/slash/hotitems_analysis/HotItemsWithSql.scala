package com.slash.hotitems_analysis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
  * Created by AnLuyao on 2020-09-01 20:42
  */
object HotItemsWithSql {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //要调用TableAPI，先创建执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)

    //将DataStream注册成表,提取需要的字段进行处理
    tableEnvironment
      .createTemporaryView("data_table", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    //用SQL实现
    val resultTable: Table = tableEnvironment.sqlQuery(
      """
        |select *
        |from (
        | select *,
        |   row_number() over (partition by windowEnd order by cnt desc) as row_num
        | from (
        |   select itemId,
        |     count(itemId) as cnt,
        |     hop_end(ts,interval '5' minute,interval '1' hour) as windowEnd
        |   from data_table
        |   where behavior='pv'
        |   group by hop(ts,interval '5' minute,interval '1' hour),itemId
        | )
        |)
        |where row_num<=5
      """.stripMargin)

    resultTable.toRetractStream[(Long, Long, Timestamp, Long)].print("result")
    environment.execute("HotItemsWithTable job")
  }

}
