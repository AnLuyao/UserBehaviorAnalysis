package com.slash.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by AnLuyao on 2020-08-30 13:24
  */
//定义输入输出数据样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件读取数据并转换成样例类
    val source = getClass.getResource("/OrderLog.csv")
    val orderEventStream: DataStream[OrderEvent] = environment.readTextFile(source.getPath)
      .map(x => {
        val strings: Array[String] = x.split(",")
        OrderEvent(strings(0).toLong, strings(1), strings(2), strings(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })

    //定义一个要匹配的事件序列的模式
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))


    //将pattern应用在按照OrderID分组的数据流上
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream.keyBy(_.orderId), orderPayPattern)

    // 定义一个侧输出流标签，用来标明超时事件的侧输出流
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("order timeout")

    //调用select方法，提取匹配事件和超时事件，分别处理转换输出
    val resultStream: DataStream[OrderResult] = patternStream
      .select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    environment.execute("order timeout detect job")
  }
}

//自定义超时处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout at " + l)
  }
}

//自定义匹配处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("pay").get(0).orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}
