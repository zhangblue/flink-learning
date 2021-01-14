package com.zhangdi.flink.scala.apitest.sink.kafka

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * @description: kafka 生产者，向kafka中写入value，没有key
 * @author zhangdi
 * @date 2021/1/10 上午12:13
 * @since ${since}
 **/
object WriteStringToKafkaExample1 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    env.fromElements("hello", "word").addSink(new FlinkKafkaProducer[String]("172.16.36.123:9092", "sink-topic", new SimpleStringSchema()))


    env.execute("job kafka sink to kafka")


  }


}


