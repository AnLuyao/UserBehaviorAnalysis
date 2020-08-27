package com.slash.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by AnLuyao on 2020-08-27 20:21
  */
case class UvCount(windowEnd: Long, count: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件读取数据
    val inputStream: DataStream[String] = environment.readTextFile("/Users/anluyao/workspace/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")

    //将数据转换为样例类，提取时间戳并且定义watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //分配key，包装成二元组开窗聚合
    val uvStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) //基于dataStream做一小时的滚动窗口统计
      //      .apply(new UvCountResult())
      .aggregate(new UvCountAgg(), new UvCountResultWithIncreAgg())

    uvStream.print()
    environment.execute("uv job")

  }
}

//自定义全窗口函数
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义一个set类型来保存所有的userId,自动去重
    var idSet = Set[Long]()
    //将当前窗口的所有数据，添加到set里
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
    //输出set的大小，就是去重之后的uv值
    out.collect(UvCount(window.getEnd, idSet.size))
  }
}

//自定义增量聚合函数，需要定义一个set作为累加状态
class UvCountAgg() extends AggregateFunction[UserBehavior, Set[Long], Long] {
  override def createAccumulator(): Set[Long] = Set[Long]()

  override def add(in: UserBehavior, acc: Set[Long]): Set[Long] = acc + in.userId

  override def getResult(acc: Set[Long]): Long = acc.size

  override def merge(acc: Set[Long], acc1: Set[Long]): Set[Long] = acc ++ acc1
}

//自定义窗口函数，添加window信息包装成样例类
class UvCountResultWithIncreAgg() extends AllWindowFunction[Long, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
    out.collect(UvCount(window.getEnd, input.head))
  }
}