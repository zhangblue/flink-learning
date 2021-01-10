package com.zhangdi.flink.scala.source.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @description: 消费kafka，默认消费没有key
 * @author zhangdi
 * @date 2021/1/7 上午12:59
 * @since ${since}
 **/
object SensorSourceFromKafkaExample1 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "172.16.36.123:9092")
    properties.setProperty("group.id", "test")

    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("test-topic", new SimpleStringSchema(), properties))
    dataStream.print("value = ")

    env.execute("my scala job")
  }

}
