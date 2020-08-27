package com.slash.networkflow_analysis


import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, RichMapFunction}
import org.apache.flink.api.common.state
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Created by AnLuyao on 2020-08-27 03:34
  */

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class PvCount(windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(4)
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
    val pvStream: DataStream[PvCount] = dataStream
      .filter(_.behavior == "pv")
      //      .map(x => ("pv", 1L)) //map成二元组
      .map(new MyMapper()) //自定义mapper,将key均匀分配
      .keyBy(_._1) //把所有数据分到一组做统计
      .timeWindow(Time.hours(1)) //开一小时的滚动窗口做统计
      .aggregate(new PvCountAgg(), new PvCountResult())

    //把各分区的结果进行汇总
    val pvTotalStream: DataStream[PvCount] = pvStream
      .keyBy(_.windowEnd)
//      .sum("count")
      .process(new TotalPvCountResult())


    pvTotalStream.print()
    environment.execute("pv job")

  }

}

class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class PvCountResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.head))
  }
}

//自定义MapFunction，随机生成key
class MyMapper() extends RichMapFunction[UserBehavior, (String, Long)] {
  lazy val indexOfThisSubtask: Int = getRuntimeContext.getIndexOfThisSubtask

  override def map(t: UserBehavior): (String, Long) = (indexOfThisSubtask.toString, 1L)
}

//自定义ProcessFunction，将聚合结果按照窗口合并
class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {
  //定义一个状态,用来保存当前所有结果之和
  lazy val totalCountState: state.ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count", classOf[Long]))

  override def processElement(i: PvCount, context: KeyedProcessFunction[Long, PvCount, PvCount]#Context, collector: Collector[PvCount]): Unit = {
    //加上新的count值，更新状态
    totalCountState.update(totalCountState.value() + i.count)
    //注册定时器 ，windowEnd+1后触发
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    //定时器触发时候，所有分区count值都已到达，输出总和
    out.collect(PvCount(ctx.getCurrentKey, totalCountState.value()))
    totalCountState.clear()
  }
}