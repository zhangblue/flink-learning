package com.zhangdi.flink.join

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 两个窗口进行join
 *
 * @author di.zhang
 * @date 2020/9/24
 * @time 18:51
 **/
object WindowJoinExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 基于间隔的join只能使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val input1: DataStream[(String, Int, Long)] = env.fromElements(
      ("a", 1, 1000L),
      ("a", 2, 2000L),
      ("b", 1, 3000L),
      ("b", 2, 4000L)
    ).assignAscendingTimestamps(_._3)

    val input2: DataStream[(String, Int, Long)] = env.fromElements(
      ("a", 10, 1000L),
      ("a", 20, 2000L),
      ("b", 10, 3000L),
      ("b", 20, 4000L)
    ).assignAscendingTimestamps(_._3)

    input1.join(input2)
      .where(_._1) // 设置input1的key
      .equalTo(_._1) // 设置input2的key
      .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 将相同key的数据进行开窗
      .apply(new MyJoin) // join操作
      .print()

    env.execute()
  }

  /**
   * 相同的key分流开窗后， 属于同一个窗口的input1中的元素和input2中的元素做笛卡尔积
   */
  class MyJoin extends JoinFunction[(String, Int, Long), (String, Int, Long), String] {
    override def join(first: (String, Int, Long), second: (String, Int, Long)): String = {
      first + " ======> " + second
    }
  }

}
