package com.zhangdi.flink.scala.apitest.window.aggregate

import com.zhangdi.flink.scala.apitest.model.SensorReading
import com.zhangdi.flink.scala.apitest.source.random.SensorSourceFromRandom
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 增量聚合函数与全窗口聚合函数混合使用
 *
 * @author di.zhang
 * @date 2020/8/24
 * @time 17:16
 **/
object AvgTempByAggAndProcWindow {

  case class AvgInfo(id: String, avgTemp: Double, widnowStart: Long, windowEnd: Long)

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取数据流
    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)
    //按照传感器的id进行分组
    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(_.id)
    //对分组结果进行开窗，计算每个传感器每5秒之内的平均温度-
    keyedStream.timeWindow(Time.seconds(5)).aggregate(new AvgTempAgg, new AvgTempProcAgg).print()

    // 启动streaming程序
    env.execute("AvgTempByAggAndProcWindow Example")
  }

  /**
   * 聚合函数
   * 第一个范型：输入数据流的类型
   * 第二个范型：累加器的类型。元组(传感器id，窗口中来了多少条数据，窗口中的温度数据总和)
   * 第三个范型：聚合函数的返回值类型。元素(传感器id，平均温度)
   */
  class AvgTempAgg extends AggregateFunction[SensorReading, (String, Long, Double), (String, Double)] {
    /**
     * 创建一个默认的累加器。
     *
     * @return
     */
    override def createAccumulator(): (String, Long, Double) = ("", 0L, 0.0)

    /**
     * 聚合时的累加函数。 每条数据都会调用
     *
     * @param value
     * @param accumulator
     * @return
     */
    override def add(value: SensorReading, accumulator: (String, Long, Double)): (String, Long, Double) = {
      (value.id, accumulator._2 + 1, accumulator._3 + value.temperature)
    }

    /**
     * 窗口关闭时所要执行的聚合方法
     *
     * @param accumulator
     * @return
     */
    override def getResult(accumulator: (String, Long, Double)): (String, Double) = {
      (accumulator._1, accumulator._3 / accumulator._2)
    }

    /**
     * 两个窗口的累加器合并函数
     *
     * @param accumulator1 累加器1
     * @param accumulator2 累加器2
     * @return
     */
    override def merge(accumulator1: (String, Long, Double), accumulator2: (String, Long, Double)): (String, Long, Double) = {
      (accumulator1._1, accumulator1._2 + accumulator2._2, accumulator1._3 + accumulator2._3)
    }
  }

  /**
   * 注意！
   * 1. 第一个范型为增量聚合函数的输出类型
   * 2. 因为全窗口函数收到数据时，同一个key在一个窗口中只有一条数据。 所以elements参数只有一个值。直接使用elements.head即可获取到
   */
  class AvgTempProcAgg extends ProcessWindowFunction[(String, Double), AvgInfo, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double)], out: Collector[AvgInfo]): Unit = {
      // 迭代器中只有一个值， 就是增量聚合函数发送过来的聚合结果
      out.collect(AvgInfo(key, elements.head._2, context.window.getStart, context.window.getEnd))
    }
  }

}
