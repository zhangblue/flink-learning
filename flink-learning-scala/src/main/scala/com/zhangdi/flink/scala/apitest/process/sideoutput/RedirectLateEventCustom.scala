package com.zhangdi.flink.scala.apitest.process.sideoutput

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 将迟到数据发送到侧输出流中
 *
 * @author di.zhang
 * @date 2020/8/30
 * @time 17:53
 **/
object RedirectLateEventCustom {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val outputTag = new OutputTag[String]("late")
    val dataStream: DataStream[(String, Long)] = env.socketTextStream("localhost", 9999, '\n')
      .filter(_.nonEmpty)
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .process(new LateEventProcess(outputTag))

    //主输出流
    dataStream.print()
    //侧输出流
    dataStream.getSideOutput(outputTag).print()

    env.execute()
  }

  class LateEventProcess(outputTag: OutputTag[String]) extends ProcessFunction[(String, Long), (String, Long)] {
    override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
      //当前数据的时间小于水位线，说明是迟到数据
      val currentMaterMark = ctx.timerService().currentWatermark()
      println("当前水位线： " + currentMaterMark)
      if (value._2 < currentMaterMark) {
        ctx.output(outputTag, "迟到数据: " + value)
      } else {
        out.collect(value)
      }
    }
  }

}
