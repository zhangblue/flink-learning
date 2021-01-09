package com.zhangdi.flink.scala.window.time.watermark

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 使用 KeyedProcessFunction 测试合流时水位线的传播规则
 *
 * @author di.zhang
 * @date 2020/8/28
 * @time 15:01
 **/
object WatermarkTest {

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置时间使用事件时间进行计算
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream1: DataStream[(String, Long)] = env.socketTextStream("localhost", 9999, '\n')
      .filter(_.nonEmpty)
      .map(new MyMapFunction)
      .assignAscendingTimestamps(_._2) // 测试数据不会发送过期时间数据， 所以此处直接使用数据时间即可
    val stream2: DataStream[(String, Long)] = env.socketTextStream("localhost", 9998, '\n')
      .filter(_.nonEmpty)
      .map(new MyMapFunction)
      .assignAscendingTimestamps(_._2) // 测试数据不会发送过期时间数据， 所以此处直接使用数据时间即可

    stream1.union(stream2)
      .keyBy(_._1)
      .process(new MyKeyedProcessFunction)
      .print()



    // 启动streaming程序
    env.execute("EventTimeExample")
  }

  class MyMapFunction extends MapFunction[String, (String, Long)] {
    override def map(value: String): (String, Long) = {
      val arr = value.split(" ")
      (arr(0), arr(1).toLong * 1000L)
    }
  }

  /**
   * 自定义 KeyedProcessFunction 函数
   */
  class MyKeyedProcessFunction extends KeyedProcessFunction[String, (String, Long), String] {

    /**
     * 每到一条数据就调用一次
     *
     * @param value 数据
     * @param ctx   上下文数据。可以获取到水位线
     * @param out   输出结果收集器
     */
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      //输出当前的水位线
      out.collect("当前数据 = " + value + " 当前水位线为：" + ctx.timerService().currentWatermark())
    }
  }

}
