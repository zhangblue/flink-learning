package com.zhangdi.flink.scala.window.time.event

import java.sql.Timestamp

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * 事件时间操作
 *
 * @author di.zhang
 * @date 2020/8/26
 * @time 19:41
 **/
object EventTimeExample {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置时间使用事件时间进行计算
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置每隔1秒插入一次水位线。系统默认每隔200毫秒插入一次
    //    env.getConfig.setAutoWatermarkInterval(1000)
    val stream: DataStream[String] = env.socketTextStream("localhost", 9999, '\n')

    val result = stream.filter(_.nonEmpty).map(new MyMapFunction)
      // 设置时间戳和水位线一定要在keyBy之前进行
      .assignTimestampsAndWatermarks(
        // 设置事件的最大延迟时间为5秒
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          // 告知系统事件时间字段为元组的第二个元素
          override def extractTimestamp(element: (String, Long)): Long = {
            element._2
          }
        })
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new WindowResult)

    result.print()


    // 启动streaming程序
    env.execute("EventTimeExample")
  }

  /**
   * 返回温度最小的数据
   */
  class MyMapFunction extends MapFunction[String, (String, Long)] {
    override def map(value: String): (String, Long) = {
      val arr = value.split(" ");
      (arr(0), arr(1).toLong * 1000)
    }
  }

  class WindowResult extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(new Timestamp(context.window.getStart) + "~" + new Timestamp(context.window.getEnd) + " 窗口中有 " + elements.size + " 个元素")
      for (elem <- elements) {
        out.collect(elem._1 + "---" + elem._2.toString)
      }
    }
  }
}
