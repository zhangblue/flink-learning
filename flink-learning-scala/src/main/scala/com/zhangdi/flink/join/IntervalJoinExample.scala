package com.zhangdi.flink.join

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * join，将两条流通过key进行join
 *
 * @author di.zhang
 * @date 2020/9/11
 * @time 15:08
 **/
object IntervalJoinExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 基于间隔的join只能使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val clickStream: KeyedStream[(String, String, Long), String] = env.fromElements(
      ("1", "click", 3600 * 1000L)
    )
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)

    val browseStream: KeyedStream[(String, String, Long), String] = env.fromElements(
      ("1", "browse", 2000 * 1000L),
      ("1", "browse", 3100 * 1000L),
      ("1", "browse", 3200 * 1000L),
      ("1", "browse", 4000 * 1000L),
      ("1", "browse", 7200 * 1000L)
    )
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)


    val joinedStreams: DataStream[String] = clickStream
      .intervalJoin(browseStream)
      //将 3600s 与 3000s ~ 4100s 的数据进行join
      .between(Time.seconds(-600), Time.seconds(500))
      .process(new MyProcessJoinFunction)

    joinedStreams.print()

    env.execute()

  }

  /**
   * 自定义ProcessJoinFunction
   */
  class MyProcessJoinFunction extends ProcessJoinFunction[(String, String, Long), (String, String, Long), String] {
    /**
     *
     * @param left  左侧的流
     * @param right 右侧的流
     * @param ctx   侧输出流
     * @param out   主输出流
     */
    override def processElement(left: (String, String, Long), right: (String, String, Long), ctx: ProcessJoinFunction[(String, String, Long), (String, String, Long), String]#Context, out: Collector[String]): Unit = {
      out.collect(left + " ========> " + right)
    }
  }

}
