package com.zhangdi.flink.scala.apitest.cep

import java.util

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction, PatternSelectFunction}
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @description: 将命中规则引擎的数据输出.
 *               1.同一个id在5秒钟内，必须经过a->b->c。
 *               2.将没有命中规则的数据从侧数据流中输出. 侧输出流
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


    env.setStateBackend(new MemoryStateBackend())

    val kafkaDataStream: DataStream[PageFrom] = env.fromElements(
      PageFrom("id1", "1.1.1.1", "create", 1000L),
      PageFrom("id1", "1.1.1.2", "create", 2000L),
      PageFrom("id1", "1.1.1.3", "create", 3000L),
      PageFrom("id1", "1.1.1.4", "pay", 4000L),
      PageFrom("id1", "1.1.1.5", "pay", 5000L),
      PageFrom("id1", "1.1.1.6", "pay2", 1000000L)
    )

    val pageFomDataStream: DataStream[PageFrom] = kafkaDataStream.assignAscendingTimestamps(_.time).keyBy(new MyKeySelector)

    val patternCondition = Pattern
      .begin[PageFrom]("start").where(new MyCondition("create"))
      .next("next1").where(new MyCondition("pay"))
      .within(Time.seconds(5))

    val patternStream: PatternStream[PageFrom] = CEP.pattern(pageFomDataStream, patternCondition)

    //超时的内容输出。scala柯里化特性
    val timeOutFunction = (map: scala.collection.Map[String, Iterable[PageFrom]], ts: Long, out: Collector[String]) => {
      val startPage1 = map("start").iterator
      while (startPage1.hasNext) {
        val one = startPage1.next()
        print("-----------" + one.ip)
      }
      print("==========")
      out.collect("超时的内容为:")
    }

    //正确的内容输出。scala柯里化特性
    val selectFunction = (map: scala.collection.Map[String, Iterable[PageFrom]], coll: Collector[String]) => {
      val start: PageFrom = map("start").iterator.next()
      val next: PageFrom = map("next1").iterator.next()

      val ret = "命中规则用户: " + start.id + " 分别在ip " + start.ip + " ; " + next.ip + " 中出现了!"
      coll.collect(ret)
    }


    val outputTag: OutputTag[String] = OutputTag("not_hit")
    //方法1
    //    patternStream.flatSelect(outputTag, new MyPatternFlatTimeoutFunction, new MyPatternFlatSelectFunction)
    //方法2，使用scala柯里化
    val functionToFunction = patternStream.flatSelect(outputTag)(timeOutFunction)(selectFunction)

    functionToFunction.print("正常的数据：")

    val sideOutPut: DataStream[String] = functionToFunction.getSideOutput(outputTag)

    sideOutPut.print("异常的数据 = ")

    env.execute("flink scala cep example2")
  }

  class MyPatternSelectFunction extends PatternSelectFunction[PageFrom, String] {
    override def select(pattern: util.Map[String, util.List[PageFrom]]): String = {
      val start = pattern.get("start").iterator().next()
      val next1 = pattern.get("next1").iterator().next()
      val next2 = pattern.get("next2").iterator().next()
      "命中规则用户: " + start.id + " 分别在ip " + start.ip + " ; " + next1.ip + " ; " + next2.ip + " 中出现了!"
    }
  }

  class MyPatternFlatTimeoutFunction extends PatternFlatTimeoutFunction[PageFrom, String] {
    override def timeout(pattern: util.Map[String, util.List[PageFrom]], timeoutTimestamp: Long, out: Collector[String]): Unit = {

    }
  }

  class MyPatternFlatSelectFunction extends PatternFlatSelectFunction[PageFrom, String] {
    override def flatSelect(pattern: util.Map[String, util.List[PageFrom]], out: Collector[String]): Unit = {

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



