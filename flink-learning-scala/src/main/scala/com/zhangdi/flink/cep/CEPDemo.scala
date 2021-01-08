package com.zhangdi.flink.cep

import java.util.Properties

import com.zhangdi.flink.model.PageFrom
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.CEP
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @description: ${todo}
 * @author zhangdi
 * @date 2021/1/8 下午2:59
 * @since ${since}
 **/
object CEPDemo {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.36.123:9092")
    properties.setProperty("group.id", "test")

    val kafkaDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("test-topic", new SimpleStringSchema(), properties))


    val value: DataStream[PageFrom] = kafkaDataStream.map(new MyMapFunction).assignAscendingTimestamps(_.time).keyBy(_.id)


    val patternCondition: Pattern[PageFrom, PageFrom] = Pattern
      .begin[PageFrom]("start").where(new MyCondition("a"))
      .next("next1").where(new MyCondition("b"))
      .next("next2").where(new MyCondition("c")).within(Time.seconds(5))

    val value1 = CEP.pattern(value, patternCondition)


    env.execute("flink scala cep")
  }


  class MyMapFunction extends MapFunction[String, PageFrom] {
    override def map(value: String): PageFrom = {
      val spValue = value.split(",")
      PageFrom(spValue(0), spValue(1), spValue(2).toLong)
    }
  }

  class MyCondition(from: String) extends SimpleCondition[PageFrom] {
    override def filter(value: PageFrom): Boolean = {
      value.from.equals(from)
    }
  }

}



