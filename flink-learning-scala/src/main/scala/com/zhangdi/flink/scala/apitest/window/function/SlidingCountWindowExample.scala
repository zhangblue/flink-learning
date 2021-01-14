package com.zhangdi.flink.scala.apitest.window.function

import com.zhangdi.flink.scala.apitest.model.SensorReading
import com.zhangdi.flink.scala.apitest.source.random.SensorSourceFromRandom
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

/**
 * 滑动计数窗口
 *
 *
 *
 * @author di.zhang
 * @date 2020/8/24
 * @time 11:13
 **/
object SlidingCountWindowExample {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度1
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)

    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    //每10条数据组成一个窗口，并且每滑动2条数据计算一次。
    //举例：1-10条组成第一个窗口， 计算一次， 当收到第13条数据时，将第3-12条数据作为第二个窗口计算一次。
    val windowedStream: WindowedStream[SensorReading, String, GlobalWindow] = keyedStream.countWindow(10, 2)

    //reduce后的得到最小的温度值数据
    val reduceStream: DataStream[SensorReading] = windowedStream.reduce(new MyReduceFunction)
    reduceStream.print()

    // 启动streaming程序
    env.execute("Session Window Example")
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
