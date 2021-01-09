package com.zhangdi.flink.scala.sink.kafka

import java.lang
import java.util.Properties

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.random.SensorSourceFromRandom
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * @description: kafka 生产者例子， 向kafka中写入key 和 value
 * @author zhangdi
 * @date 2021/1/10 上午12:51
 * @since ${since}
 **/
object WriteStringToKafkaExample2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val proper = new Properties()
    proper.setProperty("bootstrap.servers", "172.16.36.123:9092")

    env.addSource(new SensorSourceFromRandom)
      .addSink(
        new FlinkKafkaProducer[SensorReading]("test-topic",
          new MyKafkaSerializationSchema("sink-topic", "sink-topic2"),
          proper,
          FlinkKafkaProducer.Semantic.AT_LEAST_ONCE)
      )
    env.execute("job kafka sink to kafka")
  }

  private class MyKafkaSerializationSchema(topic1: String, topic2: String) extends KafkaSerializationSchema[SensorReading] {
    override def serialize(element: SensorReading, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
      val key = element.id
      val value = element.toString
      //如果id是sensor_10 则向topic1中写入数据， 否则写入到topic2中
      if (key.equals("sensor_10")) {
        new ProducerRecord[Array[Byte], Array[Byte]](topic1, key.getBytes, value.getBytes)
      } else {
        new ProducerRecord[Array[Byte], Array[Byte]](topic2, key.getBytes, value.getBytes)
      }
    }
  }

}
