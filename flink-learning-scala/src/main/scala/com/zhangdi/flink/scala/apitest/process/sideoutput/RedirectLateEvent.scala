package com.zhangdi.flink.scala.apitest.process.sideoutput

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 将迟到数据发送到侧输出流中
 *
 * @author di.zhang
 * @date 2020/8/30
 * @time 17:53
 **/
object RedirectLateEvent {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val dataStream: DataStream[String] = env.socketTextStream("localhost", 9999, '\n')
      .filter(_.nonEmpty)
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .sideOutputLateData(new OutputTag[(String, Long)]("late")) //将迟到数据放入侧输出流中
      .process(new CountFunction) //执行 ProcessWindowFunction

    //主输出流
    dataStream.print()
    //侧输出流
    dataStream.getSideOutput(new OutputTag[(String, Long)]("late")).print()

    env.execute()
  }

  class CountFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("当前窗口中的数据条数为 " + elements.size)
    }
  }

}
