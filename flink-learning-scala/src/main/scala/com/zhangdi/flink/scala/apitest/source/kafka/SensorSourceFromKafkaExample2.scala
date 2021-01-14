package com.zhangdi.flink.scala.apitest.source.kafka

import java.util.Properties

import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * @description: kafka消费者，消费带有key的数据
 * @author zhangdi
 * @date 2021/1/10 下午1:17
 * @since ${since}
 **/
object SensorSourceFromKafkaExample2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "172.16.36.123:9092")
    properties.setProperty("group.id", "test2")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val dataStream: DataStream[(String, Integer, Long, String, String)] = env.addSource(new FlinkKafkaConsumer[(String, Integer, Long, String, String)]("sink-topic2", new MyKafkaDeserializationSchema, properties))
    dataStream.print("value = ")

    env.execute("my scala job")
  }


  /**
   * 定义序列化的方式
   *
   * (topic-name,partition-num,offset,key,value)
   */
  class MyKafkaDeserializationSchema extends KafkaDeserializationSchema[(String, Integer, Long, String, String)] {
    /**
     * 定义何时结束流，因为是kafka，所以此处不结束
     *
     * @param nextElement
     * @return
     */
    override def isEndOfStream(nextElement: (String, Integer, Long, String, String)): Boolean = false

    /**
     * 获取反序列化后的数据
     *
     * @param record
     * @return
     */
    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, Integer, Long, String, String) = {
      if (record.key() == null) {
        (record.topic(), record.partition(), record.offset(), "empty", new String(record.value()))
      } else {
        (record.topic(), record.partition(), record.offset(), new String(record.key()), new String(record.value()))
      }
    }

    /**
     * 获取反序列化的对象类型
     *
     * @return
     */
    override def getProducedType: TypeInformation[(String, Integer, Long, String, String)] = {
      TypeInformation.of(new TypeHint[Tuple5[String, Integer, Long, String, String]]() {})
    }
  }

}
