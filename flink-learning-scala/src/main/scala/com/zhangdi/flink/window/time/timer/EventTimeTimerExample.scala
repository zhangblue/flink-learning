package com.zhangdi.flink.window.time.timer

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 事件时间定时器
 *
 * @author di.zhang
 * @date 2020/8/29
 * @time 21:13
 **/
object EventTimeTimerExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 9999, '\n')
      .filter(_.nonEmpty)
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .process(new MyKeyedProcessFunctionTimer)

    inputStream.print()

    env.execute()
  }

  /**
   * 自定义KeyedProcessFunction实现类
   * KeyedProcessFunction#processElement = 每条数据收到后，创建一个一个事件时间+10秒的触发器
   * KeyedProcessFunction#onTimer 触发器执行的逻辑
   */
  class MyKeyedProcessFunctionTimer extends KeyedProcessFunction[String, (String, Long), String] {
    /**
     * 每条数据来后执行
     *
     * @param value
     * @param ctx
     * @param out
     */
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      //注册定时器: 事件携带的时间戳 + 10秒
      ctx.timerService().registerEventTimeTimer(value._2 + 10 * 1000L)
    }

    /**
     * 触发器的执行逻辑，只有当超过水位线是才会执行
     *
     * @param timestamp
     * @param ctx
     * @param out
     */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(ctx.getCurrentKey + " 的定时器触发了！定时器执行的时间戳是：" + new Timestamp(timestamp))
    }
  }

}
