package com.slash.hotitems_analysis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
  * Created by AnLuyao on 2020-09-01 00:38
  */
object HotItemsWithTable {
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
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment,settings)

    //将DataStream转换成表,提取需要的字段进行处理
    val dataTable = tableEnvironment
      .fromDataStream(dataStream,'itemId,'behavior,'timestamp.rowtime as 'ts)

    //分组开窗增量聚合
    val aggTable: Table = dataTable.filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'itemId.count as 'cnt, 'sw.end as 'windowEnd)

    //用SQL实现分组选取TopN的功能
    tableEnvironment.createTemporaryView("agg",aggTable,'itemId,'cnt,'windowEnd)
    val resultTable: Table = tableEnvironment.sqlQuery(
      """
        |select *
        |from (
        | select *,
        |   row_number() over (partition by windowEnd order by cnt desc) as row_num
        | from agg
        |)
        |where row_num<=5
      """.stripMargin)

    resultTable.toRetractStream[(Long,Long,Timestamp,Long)].print("result")
    environment.execute("HotItemsWithTable job")

  }

}
