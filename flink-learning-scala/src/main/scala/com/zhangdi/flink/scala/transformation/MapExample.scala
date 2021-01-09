package com.zhangdi.flink.scala.transformation

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.SensorSourceFromRandom
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

/**
 * map 函数 demo
 *
 * @author di.zhang
 * @date 2020/8/20
 * @time 19:44
 **/
object MapExample {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSourceFromRandom)

    stream.map(new MyMapFunction).print()

    env.execute("map example")
  }

  /**
   * 自定义MapFunction. 输入范型SensorReading 输出范型String
   *
   *
   */
  class MyMapFunction extends MapFunction[SensorReading, String] {
    override def map(value: SensorReading): String = value.id
  }

}
