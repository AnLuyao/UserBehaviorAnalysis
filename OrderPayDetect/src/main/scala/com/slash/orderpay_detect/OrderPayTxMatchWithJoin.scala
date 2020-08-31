package com.slash.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Created by AnLuyao on 2020-08-31 23:27
  */
object OrderPayTxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件读取数据并转换成样例类
    val resource1 = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, String] = environment.readTextFile(resource1.getPath)
      .map(x => {
        val strings: Array[String] = x.split(",")
        OrderEvent(strings(0).toLong, strings(1), strings(2), strings(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .filter(_.txId != "")
      .keyBy(_.txId)

    val resource2 = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream: KeyedStream[ReceiptEvent, String] = environment.readTextFile(resource2.getPath)
      .map(x => {
        val strings: Array[String] = x.split(",")
        ReceiptEvent(strings(0), strings(1), strings(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.timestamp * 1000L
      })
      .keyBy(_.txId)

    //使用join连接两条流
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
      .intervalJoin(receiptEventStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new OrderPayTxDetectWithJoin())

    resultStream.print()
    environment.execute("OrderPayTxMatchWithJoin job")
  }

}

//自定义ProcessJoinFunction
class OrderPayTxDetectWithJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}