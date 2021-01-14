package com.zhangdi.flink.scala.apitest.window.function

import com.zhangdi.flink.scala.apitest.model.SensorReading
import com.zhangdi.flink.scala.apitest.source.random.SensorSourceFromRandom
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 滑动窗口demo， 窗口长度5s，滑动长度3s
 *
 * @author di.zhang
 * @date 2020/8/21
 * @time 18:22
 **/
object SlidingWindowsExample {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度1
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)

    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    val windowedStream: WindowedStream[SensorReading, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(5), Time.seconds(3))

    //reduce后的得到最小的温度值数据
    val reduceStream: DataStream[SensorReading] = windowedStream.reduce(new MyReduceFunction)
    reduceStream.print()

    // 启动streaming程序
    env.execute("Sliding Window Example")
  }

  /**
   * 返回温度最小的数据
   */
  class MyReduceFunction extends ReduceFunction[SensorReading] {
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
      SensorReading(value1.id, 0L, value1.temperature.min(value2.temperature))
    }
  }

}
