package com.zhangdi.flink.scala.window.aggregate

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.SensorSourceFromRandom
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 全窗口函数
 *
 * 计算平均温度
 *
 * @author di.zhang
 * @date 2020/8/24
 * @time 15:47
 **/
object AvgTempByProcessWindowFunction {

  /**
   * 自定义返回值类型
   *
   * @param id          传感器id
   * @param avgTemp     窗口的平均温度
   * @param widnowStart 窗口开始时间
   * @param windowEnd   窗口结束时间
   */
  case class AvgInfo(id: String, avgTemp: Double, widnowStart: Long, windowEnd: Long)

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取数据流
    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)
    //按照传感器的id进行分组
    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(_.id)
    //对分组结果进行开窗，计算每个传感器每5秒之内的平均温度-
    keyedStream.timeWindow(Time.seconds(5)).process(new AvgTempAgg).print()

    // 启动streaming程序
    env.execute("Process Window Function Example")
  }

  /**
   * 自定义全窗口函数
   *
   * 第一个范型：输入流的类型
   * 第二个范型：输出的类型
   * 第三个范型：key的值， 此处为传感器的id
   * 第四个范型：表示计算结果数据哪个window的数据。window信息
   *
   * 相比增量聚合函数：
   * 缺点：全窗口函数要保存窗口中的所有元素。增量聚合函数只需要保存一个累加器即可
   * 优点：全窗口聚合函数可以访问窗口信息
   **/
  class AvgTempAgg extends ProcessWindowFunction[SensorReading, AvgInfo, String, TimeWindow] {
    /**
     * 在窗口闭合时调用
     *
     * @param key      传感器id
     * @param context  上下文数据
     * @param elements 窗口中所有的数据迭代器
     * @param out      输出
     */
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[AvgInfo]): Unit = {
      val count = elements.size //窗口关闭时温度一共多少条
      var sum = 0.0 //总的温度值
      for (elem <- elements) {
        sum += elem.temperature
      }
      val windowStart = context.window.getStart // window的开始时间
      val windowEnd = context.window.getEnd // window的结束时间
      out.collect(AvgInfo(key, sum / count, windowStart, windowEnd))
    }
  }

}
