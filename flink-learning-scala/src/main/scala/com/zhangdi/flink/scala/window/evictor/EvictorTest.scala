package com.zhangdi.flink.scala.window.evictor

import java.lang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.util.Collector

/**
 * 清理器测试
 *
 * @author di.zhang
 * @date 2020/9/11
 * @time 10:51
 **/
object EvictorTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.socketTextStream("localhost", 9999, '\n')
      .map(x => {
        val arr = x.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .evictor(new MyEvictor)
      .process(new MyProcessWindowFunction)

    env.execute()
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      println("当前窗口中有 " + elements.size + " 个元素")
    }
  }

  class MyEvictor extends Evictor[(String, Long), TimeWindow] {
    override def evictBefore(elements: lang.Iterable[TimestampedValue[(String, Long)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
      println("在之前执行了...... size = " + size)
    }

    override def evictAfter(elements: lang.Iterable[TimestampedValue[(String, Long)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
      println("在之后执行了...... size = " + size)
    }
  }

}
