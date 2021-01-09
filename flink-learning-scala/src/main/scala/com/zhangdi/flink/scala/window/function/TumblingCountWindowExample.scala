package com.zhangdi.flink.scala.window.function

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.SensorSourceFromRandom
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{WindowedStream, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

/**
 * 滚动计数窗口
 * 每5条数据组成一个窗口，并计算
 *
 * @author di.zhang
 * @date 2020/8/24
 * @time 11:09
 **/
object TumblingCountWindowExample {

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度1
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)

    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    //每5条数据组成一个窗口
    val windowedStream: WindowedStream[SensorReading, String, GlobalWindow] = keyedStream.countWindow(5)

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
