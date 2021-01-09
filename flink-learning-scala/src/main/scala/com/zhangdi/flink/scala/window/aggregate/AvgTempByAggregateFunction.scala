package com.zhangdi.flink.scala.window.aggregate

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.SensorSourceFromRandom
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 增量聚合函数
 * 计算平均温度
 *
 * @author di.zhang
 * @date 2020/8/24
 * @time 12:41
 **/
object AvgTempByAggregateFunction {

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取数据流
    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)
    //按照传感器的id进行分组
    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(_.id)
    //对分组结果进行开窗，计算每个传感器每5秒之内的平均温度-
    keyedStream.timeWindow(Time.seconds(5)).aggregate(new AvgTempAgg).print()

    // 启动streaming程序
    env.execute("Session Window Example")
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
     * @param a
     * @param b
     * @return
     */
    override def merge(a: (String, Long, Double), b: (String, Long, Double)): (String, Long, Double) = {
      (a._1, a._2 + b._2, a._3 + b._3)
    }
  }

}
