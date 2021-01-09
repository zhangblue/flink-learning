package com.zhangdi.flink.scala.richfunction

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * 富函数 richFunction demo
 *
 * @author di.zhang
 * @date 2020/8/21
 * @time 16:10
 **/
object RichFunctionExample {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    println("当前并行度：" + env.getParallelism)

    env.setParallelism(2)
    val stream: DataStream[String] = env.fromElements("hello", "word")
    stream.map(new MyRichMapFunction).print()
    env.execute("rich function example")
  }

  /**
   * 自定义rich function函数
   */
  class MyRichMapFunction extends RichMapFunction[String, String] {
    /**
     * open 函数每个并行度只会执行一次
     *
     * @param parameters
     */
    override def open(parameters: Configuration): Unit = {
      println("run open function")
    }

    override def map(value: String): String = {
      value
    }

    override def close(): Unit = {
      println("run close function")
    }
  }

}
