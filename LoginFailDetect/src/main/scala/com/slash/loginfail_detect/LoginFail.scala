package com.slash.loginfail_detect

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Created by AnLuyao on 2020-08-29 15:55
  */
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningTag: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[LoginEvent] = environment.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    //用ProcessFunction进行转换，如果遇到2s内连续两次登录失败，就输出告警
    val loginWarningStream: DataStream[Warning] = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWarning(2))


    loginWarningStream.print()
    environment.execute("login fail job")

  }
}

//实现自定义的ProcessFunction
class LoginFailWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  //定义listState，状态用来保存2秒内所有的登录失败事件
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved-loginfail", classOf[LoginEvent]))
  //定义ValueState，用来保存定时器的时间戳
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    //判断当前数据是否是登录失败
    if (value.eventType == "fail") {
      //如果失败，那么添加到ListState，如果未注册果定时器，则注册
      loginFailListState.add(value)
      if (timerTsState.value() == 0) {
        val ts = value.eventTime * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      //如果登陆成功，删除定时器，重新开始
      ctx.timerService().deleteProcessingTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    //如果2s内的定时器触发了，那么判断ListState中失败的个数
    val allLoginFailList = new ListBuffer[LoginEvent]
    val iter: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
    while (iter.hasNext) {
      allLoginFailList += iter.next()
      if (allLoginFailList.length >= maxFailTimes) {
        out.collect(
          Warning(ctx.getCurrentKey,
            allLoginFailList.head.eventTime,
            allLoginFailList.last.eventTime,
            "login fail in 2s for " + allLoginFailList.length + " times.")
        )
      }
      //清空状态
      loginFailListState.clear()
      timerTsState.clear()
    }
  }
}