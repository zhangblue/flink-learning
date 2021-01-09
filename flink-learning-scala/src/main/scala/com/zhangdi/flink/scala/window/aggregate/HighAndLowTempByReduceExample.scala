package com.zhangdi.flink.scala.window.aggregate

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.random.SensorSourceFromRandom
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 使用reduce函数+全窗口函数计算最小最大温度
 *
 * @author di.zhang
 * @date 2020/8/24
 * @time 18:31
 **/
object HighAndLowTempByReduceExample {

  case class HighAndLowResult(id: String, min: Double, max: Double, windowStartTime: Long, windowEndTime: Long);

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取数据流
    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)
    //按照传感器的id进行分组
    val keyedStream: KeyedStream[(String, Double, Double), String] = stream.map(data => (data.id, data.temperature, data.temperature)).keyBy(_._1)
    //对分组结果进行开窗，计算每个传感器每5秒之内的平均温度
    keyedStream.timeWindow(Time.seconds(5)).reduce(new MyReduceFunction, new HighAndLogProcAgg).print()

    // 启动streaming程序
    env.execute("HighAndLowTempExample")
  }

  /**
   * 使用reduce函数计算窗口中的最小温度与最大温度
   */
  class MyReduceFunction extends ReduceFunction[(String, Double, Double)] {
    override def reduce(value1: (String, Double, Double), value2: (String, Double, Double)): (String, Double, Double) = {
      (value1._1, value1._2.min(value2._2), value1._3.max(value2._3))
    }
  }

  /**
   * 使用全窗口聚合函数将window数据打入结果数据中
   */
  class HighAndLogProcAgg extends ProcessWindowFunction[(String, Double, Double), HighAndLowResult, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[HighAndLowResult]): Unit = {
      out.collect(HighAndLowResult(key, elements.head._2, elements.head._3, context.window.getStart, context.window.getEnd))
    }
  }

}
