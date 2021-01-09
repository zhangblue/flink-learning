package com.zhangdi.flink.scala.window.function

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.random.SensorSourceFromRandom
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 会话窗口
 *
 * @author di.zhang
 * @date 2020/8/24
 * @time 11:03
 **/
object SessionWindowExample {

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度1
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)

    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    //创建一个超时时间为10分钟的会话窗口。
    val windowedStream: WindowedStream[SensorReading, String, TimeWindow] = keyedStream.window(EventTimeSessionWindows.withGap(Time.minutes(10)))

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
