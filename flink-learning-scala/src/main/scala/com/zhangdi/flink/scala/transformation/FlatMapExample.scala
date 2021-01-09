package com.zhangdi.flink.scala.transformation

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * flatmap函数demo
 *
 * @author di.zhang
 * @date 2020/8/20
 * @time 20:26
 **/
object FlatMapExample {

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.fromElements("white", "gray", "black")

    stream.flatMap(new MyFlatMapFunction).print


    env.execute("flatMap Example")
  }

  /**
   * 自定义FlatMapFunction
   */
  class MyFlatMapFunction extends FlatMapFunction[String, String] {
    override def flatMap(value: String, out: Collector[String]): Unit = {
      if (value.equals("white")) {
        out.collect(value)
      } else if (value.equals("black")) {
        out.collect(value)
        out.collect(value)
      }
    }
  }

}
