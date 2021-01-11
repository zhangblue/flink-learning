package com.zhangdi.flink.scala.cep

import java.util
import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @description: 将命中规则引擎的数据输出.
 *               1.同一个id在5秒钟内，必须经过a->b->c。
 *               2.将没有命中规则的数据从侧数据流中输出
 * @author zhangdi
 * @date 2021/1/8 下午2:59
 * @since ${since}
 **/
object CEPExample2 {

  case class PageFrom(id: String, ip: String, from: String, time: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.36.123:9092")
    properties.setProperty("group.id", "test")
    properties.setProperty("auto.offset.reset", "latest")


    val kafkaDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("test-topic", new SimpleStringSchema(), properties))

    val pageFomDataStream: DataStream[PageFrom] = kafkaDataStream.map(new MyMapFunction).assignAscendingTimestamps(_.time).keyBy(new MyKeySelector)

    val patternCondition = Pattern
      .begin[PageFrom]("start").where(new MyCondition("a"))
      .next("next1").where(new MyCondition("b"))
      .next("next2").where(new MyCondition("c"))
      .within(Time.seconds(5))

    val patternStream: PatternStream[PageFrom] = CEP.pattern(pageFomDataStream, patternCondition)



    val dataStream: DataStream[String] = patternStream.select(new MyPatternSelectFunction)

    dataStream.print("value = ")


    env.execute("flink scala cep")
  }

  class MyPatternSelectFunction extends PatternSelectFunction[PageFrom, String] {
    override def select(pattern: util.Map[String, util.List[PageFrom]]): String = {
      val start = pattern.get("start").iterator().next()
      val next1 = pattern.get("next1").iterator().next()
      val next2 = pattern.get("next2").iterator().next()
      "命中规则用户: " + start.id + " 分别在ip " + start.ip + " ; " + next1.ip + " ; " + next2.ip + " 中出现了!"
    }
  }


  /**
   * 自定义map函数，将字符串转为PageFrom对象
   */
  class MyMapFunction extends MapFunction[String, PageFrom] {
    override def map(value: String): PageFrom = {
      val spValue = value.split(",")
      PageFrom(spValue(0), spValue(1), spValue(2), spValue(3).toLong)
    }
  }

  /**
   * 自定义条件函数。 在CEP.where中使用
   *
   * @param from
   */
  class MyCondition(from: String) extends IterativeCondition[PageFrom] {
    override def filter(value: PageFrom, ctx: IterativeCondition.Context[PageFrom]): Boolean = {
      value.from.equals(from)
    }
  }

  /**
   * key分配器。用于DataStream.keyBy中使用
   */
  class MyKeySelector extends KeySelector[PageFrom, String] {
    override def getKey(value: PageFrom): String = {
      value.id
    }
  }

}



