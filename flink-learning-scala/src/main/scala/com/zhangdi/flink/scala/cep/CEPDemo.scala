package com.zhangdi.flink.scala.cep

import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
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

  case class PageFrom(id: String, from: String, time: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.36.123:9092")
    properties.setProperty("group.id", "test")

    val kafkaDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("test-topic", new SimpleStringSchema(), properties))

    val pageFomDataStream: DataStream[PageFrom] = kafkaDataStream.map(new MyMapFunction).assignAscendingTimestamps(_.time).keyBy(new MyKeySelector)

    val patternCondition = Pattern
      .begin[PageFrom]("start").where(new MyCondition("a"))
      .next("next1").where(new MyCondition("b"))
      .next("next2").where(new MyCondition("c"))
      .within(Time.seconds(5))

    val value1 = CEP.pattern(pageFomDataStream, patternCondition)


    env.execute("flink scala cep")
  }


  class MyMapFunction extends MapFunction[String, PageFrom] {
    override def map(value: String): PageFrom = {
      val spValue = value.split(",")
      PageFrom(spValue(0), spValue(1), spValue(2).toLong)
    }
  }

  class MyCondition(from: String) extends IterativeCondition[PageFrom] {
    override def filter(value: PageFrom, ctx: IterativeCondition.Context[PageFrom]): Boolean = {
      value.from.equals(from)
    }
  }

  class MyKeySelector extends KeySelector[PageFrom, String] {
    override def getKey(value: PageFrom): String = {
      value.id
    }
  }


}



