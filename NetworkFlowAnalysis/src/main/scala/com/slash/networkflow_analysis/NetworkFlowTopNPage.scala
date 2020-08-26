package com.slash.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
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
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })

    //开窗聚合
    val aggStream = dataStream
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new PageCountAgg(), new PageCountWindowResult())

    //每个窗口的统计值排序输出
    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPage(3))

    resultStream.print()
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
  //定义ListState保存所有聚合结果
  lazy val pageCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pagecount-list", classOf[PageViewCount]))

  override def processElement(i: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {
    pageCountListState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  //等到数据都到齐，从状态中取出排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allPageCountList: ListBuffer[PageViewCount] = ListBuffer()
    val iter = pageCountListState.get().iterator()
    while (iter.hasNext) {
      allPageCountList += iter.next()

      pageCountListState.clear()
      val sortedPageCountList = allPageCountList.sortWith(_.count > _.count).take(n)

      //将排名信息格式化成String,方便监控显示
      val result: StringBuilder = new StringBuilder
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      //便利sorted列表，输出TopN信息
      for (i <- sortedPageCountList.indices) {
        //获取当前商品的cunt信息
        val currentItemCount = sortedPageCountList(i)
        result.append("Top").append(i + 1).append(":")
          .append(" 页面URL=").append(currentItemCount.url)
          .append(" 访问量=").append(currentItemCount.count)
          .append("\n")
      }
      result.append("==============================================\n\n")
      //控制输出频率
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }
}