package com.slash.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by AnLuyao on 2020-08-30 00:23
  */
object LoginFailWithCEP {
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

    //1.定义匹配的模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail") //第一次登陆失败
//      .times(3) //非严格紧邻
      .next("secondFail").where(_.eventType == "fail") //第二次登陆失败
      .within(Time.seconds(2)) //在2秒钟之内检测匹配

    //2.在分组之后的数据流上应用pattern,得到一个PatternStream
    val pattrenStream: PatternStream[LoginEvent] = CEP
      .pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    //3.将检测到的时间序列，转换输出告警信息
    val loginFailStream: DataStream[Warning] = pattrenStream.select(new LoginFailDetect())

    loginFailStream.print()
    environment.execute("login fail with CEP job")
  }
}

//自定义PatternSelectFunction，用来将检测到的连续登陆失败事件，包装成告警信息输出
class LoginFailDetect() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    //map存放的就是匹配到的一组事件，key是定义好的事件模式名称
    val firstLoginFail = map.get("firstFail").get(0)
    val secondLoginFail = map.get("secondFail").get(0)
//    val lastLoginFail = map.get("firstFail").get(2)
    Warning(firstLoginFail.userId, firstLoginFail.eventTime, secondLoginFail.eventTime, "login fail!")
//    Warning(firstLoginFail.userId, firstLoginFail.eventTime, lastLoginFail.eventTime, "login fail!")
  }
}