package com.slash.market_analysis


import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Created by AnLuyao on 2020-08-29 12:54
  */

//定义输入数据样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

//定义输出统计的样例类
case class MarketCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

//自定义测试数据源
class SimulateMarketEventSource() extends RichParallelSourceFunction[MarketUserBehavior] {
  //定义是否在运行的标志位
  var running = true
  //定义用户行为和渠道的集合
  val behaviorSet: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  val channelSet: Seq[String] = Seq("appstore", "huaweistore", "weibo", "wechat")
  //定义随机数生成器
  val rand: Random = Random

  override def run(sourceContext: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    //定义一个发出数据的最大量，用于控制测试数据量
    val maxCOunts = Long.MaxValue
    var count = 0L
    while (running && count < maxCOunts) {
      val id: String = UUID.randomUUID().toString
      val behavior: String = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel: String = channelSet(rand.nextInt(channelSet.size))
      val ts: Long = System.currentTimeMillis()
      sourceContext.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(50L)
    }
  }

  override def cancel(): Unit = running = false
}

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketUserBehavior] = environment
      .addSource(new SimulateMarketEventSource())
      .assignAscendingTimestamps(_.timestamp)

    val resultStream: DataStream[MarketCount] = dataStream
      .filter(_.behavior != "UNINSTALL")
      .keyBy(x => (x.channel, x.behavior)) //按照渠道和行为类型分组
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketCountByChannel()) //自定义全窗口函数

    resultStream.print()
    environment.execute("market count by channel job")

  }

}

//自定义ProcessWindowFunction
class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketCount]): Unit = {
    val windowStart: String = new Timestamp(context.window.getStart).toString
    val windowEnd: String = new Timestamp(context.window.getEnd).toString
    val channel: String = key._1
    val behavior: String = key._2
    val count: Long = elements.size
    out.collect(MarketCount(windowStart, windowEnd, channel, behavior, count))
  }
}