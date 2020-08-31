package com.slash.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Created by AnLuyao on 2020-08-31 22:12
  */

//定义到账数据的样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)

object OrderPayTxMatch {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //从文件读取数据并转换成样例类
    val resource1 = getClass.getResource("/OrderLog.csv")
    val orderEventStream: DataStream[OrderEvent] = environment.readTextFile(resource1.getPath)
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
    val receiptEventStream: DataStream[ReceiptEvent] = environment.readTextFile(resource2.getPath)
      .map(x => {
        val strings: Array[String] = x.split(",")
        ReceiptEvent(strings(0), strings(1), strings(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.timestamp * 1000L
      })
      .keyBy(_.txId)

    //用connect连接两条流，匹配事件进行处理
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
      .connect(receiptEventStream)
      .process(new OrderPayTxDetect())


    val unmatchPays = new OutputTag[OrderEvent]("unmatch-pays")
    val unmatchReceipts = new OutputTag[ReceiptEvent]("unmatch-receipts")

    resultStream.print("matched")
    resultStream.getSideOutput(unmatchPays).print("unmatch-pays")
    resultStream.getSideOutput(unmatchReceipts).print("unmatch-receipts")

    environment.execute("OrderPayTxMatch job")
  }

}

//自定义CoProcessFunction，实现两条流数据匹配的检验
class OrderPayTxDetect() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  //用两个ValueState用来保存当前交易对应的支付事件和到账事件
  lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

  val unmatchPays = new OutputTag[OrderEvent]("unmatch-pays")
  val unmatchReceipts = new OutputTag[ReceiptEvent]("unmatch-receipts")

  override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //pay来了，考察有没有对应的receipt来过
    val receiptEvent: ReceiptEvent = receiptState.value()
    if (receiptEvent != null) {
      //若已经有receipt，则正常匹配，输出到主流
      out.collect((value, receiptEvent))
      receiptState.clear()
    } else {
      //若receipt没来，则把pay存入状态，注册一个定时器开始等待5s
      payState.update(value)
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
    }
  }

  override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

    val orderEvent: OrderEvent = payState.value()
    if (orderEvent != null) {
      out.collect((orderEvent, value))
      payState.clear()
    } else {
      receiptState.update(value)
      ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 3000L)
    }
  }

  /**
    * 定时器触发的两种情况，所以要判断当前有没有pay和receipt
    *
    * @param timestamp
    * @param ctx
    * @param out
    */
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    //如果pay不为空，则说明receipt没来，输出unmatchPays
    if (payState.value() != null) {
      ctx.output(unmatchPays, payState.value())
    }
    if (receiptState.value() != null) {
      ctx.output(unmatchReceipts, receiptState.value())
    }
    payState.clear()
    receiptState.clear()
  }
}