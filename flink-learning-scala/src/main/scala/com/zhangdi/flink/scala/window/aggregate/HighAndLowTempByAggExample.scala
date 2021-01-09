package com.zhangdi.flink.scala.window.aggregate

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.SensorSourceFromRandom
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 使用滚动窗口函数+全窗口函数计算最大温度与最小温度
 *
 * @author di.zhang
 * @date 2020/8/24
 * @time 17:40
 **/
object HighAndLowTempByAggExample {

  case class HighAndLowResult(id: String, min: Double, max: Double, windowStartTime: Long, windowEndTime: Long);

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取数据流
    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)
    //按照传感器的id进行分组
    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(_.id)
    //对分组结果进行开窗，计算每个传感器每5秒之内的平均温度-
    keyedStream.timeWindow(Time.seconds(5)).aggregate(new HighAndLowAgg, new HighAndLogProcAgg).print()

    // 启动streaming程序
    env.execute("HighAndLowTempExample")
  }

  /**
   * 使用增量聚合函数计算窗口中的最小温度与最大温度
   */
  class HighAndLowAgg extends AggregateFunction[SensorReading, (String, Double, Double), (String, Double, Double)] {
    override def createAccumulator(): (String, Double, Double) = ("", Double.MaxValue, Double.MinValue)

    override def add(value: SensorReading, accumulator: (String, Double, Double)): (String, Double, Double) = {
      (value.id, value.temperature.min(accumulator._2), value.temperature.max(accumulator._3))
    }

    override def getResult(accumulator: (String, Double, Double)): (String, Double, Double) = {
      (accumulator._1, accumulator._2, accumulator._3)
    }

    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = {
      (a._1, a._2.min(b._2), a._3.max(b._3))
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
