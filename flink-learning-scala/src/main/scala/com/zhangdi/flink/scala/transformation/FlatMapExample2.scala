package com.zhangdi.flink.scala.transformation

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author di.zhang
 * @date 2020/8/30
 * @time 01:19
 **/
object FlatMapExample2 {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements("a,b,c", "1,2,3")

    stream.flatMap(new MyFlatMapFunction).print


    env.execute("flatMap2 Example")
  }

  /**
   * 自定义FlatMapFunction
   */
  class MyFlatMapFunction extends FlatMapFunction[String, String] {
    override def flatMap(value: String, out: Collector[String]): Unit = {
      val arr = value.split(",")
      arr.foreach(out.collect(_))
    }
  }

}
