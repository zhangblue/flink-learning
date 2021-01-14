package com.zhangdi.flink.scala.apitest.transformation

import com.zhangdi.flink.scala.apitest.model.SensorReading
import com.zhangdi.flink.scala.apitest.source.random.SensorSourceFromRandom
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
 * filter 函数 demo
 *
 * @author di.zhang
 * @date 2020/8/20
 * @time 19:44
 **/
object FilterExample {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)

    stream.filter(new MyFilterFunction).print()

    env.execute("filter example")
  }

  /**
   * 自定义MapFunction. 输入范型SensorReading
   */
  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = value.temperature > 0
  }

}
