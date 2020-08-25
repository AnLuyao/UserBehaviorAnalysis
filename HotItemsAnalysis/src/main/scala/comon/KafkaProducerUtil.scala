package comon

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

/**
  * Created by AnLuyao on 2020-08-25 23:27
  */
object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {
    writeToKafkaWithTopic("hotitems")
  }

  def writeToKafkaWithTopic(topic: String) = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //创建一个kafkaProducer
    val producer = new KafkaProducer[String, String](properties)
    //从文件中读取数据，逐条发送
    val bufferedSource: BufferedSource = io.Source.fromFile("/Users/anluyao/workspace/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
