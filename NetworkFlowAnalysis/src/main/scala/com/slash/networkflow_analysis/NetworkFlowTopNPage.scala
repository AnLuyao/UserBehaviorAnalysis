package com.slash.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Map

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
  * Created by AnLuyao on 2020-08-26 23:45
  */
//定义输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//定义聚合结果样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlowTopNPage {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    val inputStream = environment.readTextFile("/Users/anluyao/workspace/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/apache.log")
    val dataStream = inputStream.map(data => {
      val dataArray: Array[String] = data.split(" ")
      val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp = simpleDateFormat.parse(dataArray(3)).getTime
      ApacheLogEvent(dataArray(0), dataArray(1), timestamp, dataArray(5), dataArray(6))
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })

    //开窗聚合
    val lateOutputTag = new OutputTag[ApacheLogEvent]("late data")
    val aggStream = dataStream
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(lateOutputTag)
      .aggregate(new PageCountAgg(), new PageCountWindowResult())

    //获取侧输出流
    val lateDataStream: DataStream[ApacheLogEvent] = aggStream.getSideOutput(lateOutputTag)

    //每个窗口的统计值排序输出
    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPage(3))

    dataStream.print("data")
    aggStream.print("agg")
    lateDataStream.print("late")
    resultStream.print("result")
    environment.execute("top n page job")

  }

}

//自定义预聚合函数
class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//自定义WindowFunction，包装成样例类输出
class PageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.head))
  }
}

//自定义Process Function
class TopNHotPage(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
  //定义MapState保存所有聚合结果
  lazy val pageCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pagecount-map", classOf[String], classOf[Long]))

  override def processElement(i: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {
    pageCountMapState.put(i.url, i.count)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    context.timerService().registerEventTimeTimer(i.windowEnd + 60 * 1000L)
  }

  //等到数据都到齐，从状态中取出排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    if (timestamp == ctx.getCurrentKey + 60 * 1000L) {
      pageCountMapState.clear()
      return
    }
    val allPageCountList: ListBuffer[(String, Long)] = ListBuffer()
    val iter = pageCountMapState.entries().iterator()
    while (iter.hasNext) {
      val entry: Map.Entry[String, Long] = iter.next()
      allPageCountList += ((entry.getKey, entry.getValue))
    }
    val sortedPageCountList = allPageCountList.sortWith(_._2 > _._2).take(n)

    //将排名信息格式化成String,方便监控显示
    val result: StringBuilder = new StringBuilder
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    //便利sorted列表，输出TopN信息
    for (i <- sortedPageCountList.indices) {
      //获取当前商品的cunt信息
      val currentItemCount = sortedPageCountList(i)
      result.append("Top").append(i + 1).append(":")
        .append(" 页面URL=").append(currentItemCount._1)
        .append(" 访问量=").append(currentItemCount._2)
        .append("\n")
    }
    result.append("==============================================\n\n")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
    //    }
  }
}