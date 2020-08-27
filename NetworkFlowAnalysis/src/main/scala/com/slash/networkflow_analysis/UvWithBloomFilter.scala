package com.slash.networkflow_analysis

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * Created by AnLuyao on 2020-08-27 21:33
  */
object UvWithBloomFilter {
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
      .map(x => ("uv", x.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) //自定义trigger
      .process(new UvCountResultWithBloomFilter())


    uvStream.print()
    environment.execute("uv with bloom filter job")
  }

}

//自定义一个触发器，每来一条数据就触发一次窗口计算
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  //数据来了之后触发计算并清空状态，不保存数据
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
}

//自定义ProcessWindowFunction,把当前数据进行处理，位图保存在Redis中
class UvCountResultWithBloomFilter() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  var jedis: Jedis = _
  var bloom: Bloom = _

  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("localhost",6379)
    //位图大小10亿个位，也就是 2^30,占用128M
    bloom = new Bloom(1 << 30)
  }

  //每来一个数据，主要是要用布隆过滤器判断Redis位图中对应位置是否为1
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //bitmap用当前窗口的windowend作为key,保存到Redis里（windowend，bitmap）
    val storedKey: String = context.window.getEnd.toString
    //把每个窗口的uv count值，作为状态也存入Redis中，存成countMap的表
    val countMap = "countMap"
    //先获取当前的count值
    var count = 0L
    if (jedis.hget(countMap, storedKey) != null) {
      count = jedis.hget(countMap, storedKey).toLong
    }
    //取userID，计算hash值，判断是否在位图中
    val userId = elements.head._2.toString
    val offset = bloom.hash(userId, 61)
    val isExist = jedis.getbit(storedKey, offset)
    //如果不存在，那么就将对应位置置1，count+1,如果不存在，不操作
    if (!isExist) {
      jedis.setbit(storedKey, offset, true)
      jedis.hset(countMap, storedKey, count + 1.toString)
    }
  }
}

//自定义一个布隆过滤器
class Bloom(size: Long) extends Serializable {
  //定义位图的大小,应该是2的整次幂
  private val cap = size

  //实现一个hash函数
  def hash(str: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until (str.length)) {
      result = result * seed + str.charAt(i)
    }
    //返回一个在cap范围内的值
    (cap - 1) & result
  }
}