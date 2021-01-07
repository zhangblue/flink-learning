package com.zhangdi.flink.wc

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * flink streaming word count程序
 * 通过从socket 9999端口中读取数据，以每5秒一个窗口做一次分组， 计算word count
 *
 * @author di.zhang
 * @date 2020/8/19
 * @time 13:32
 **/
object WordCountFromSocket {

  case class WordWithCount(word: String, count: Int)

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度1
    env.setParallelism(1)
    // 从socket中读取数据， 需要先启动 `nc -lk 9999`, 再启动程序。
    val stream = env.socketTextStream("172.16.36.134", 9998, '\n')

    // 读取数据， 按照空格分割
    val windowCounts = stream.flatMap(line => line.split("\\s"))
      // 封装成所要的格式
      .map(x => WordWithCount(x, 1))
      // 按照word字段进行分组
      .keyBy(new MyKeyBySelector)
      // 开5秒的滚动窗口
      .timeWindow(Time.seconds(5))
      // 按照第二个字段count求和
      .sum("count")


    // 打印结果
    windowCounts.print().setParallelism(1)

    // 启动streaming程序
    env.execute("Socket Window WordCount")
  }

  /**
   * 自定义keySelector， 用于keyBy函数使用
   */
  class MyKeyBySelector extends KeySelector[WordWithCount, String] {
    override def getKey(value: WordWithCount): String = value.word
  }

}


