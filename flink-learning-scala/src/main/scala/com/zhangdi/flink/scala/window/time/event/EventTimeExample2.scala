package com.zhangdi.flink.scala.window.time.event

import java.sql.Timestamp

import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 自定义watermark生成策略
 *
 * @author di.zhang
 * @date 2020/8/27
 * @time 19:02
 **/
object EventTimeExample2 {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置时间使用事件时间进行计算
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置每隔1秒插入一次水位线。系统默认每隔200毫秒插入一次
    env.getConfig.setAutoWatermarkInterval(5000)
    val stream: DataStream[String] = env.socketTextStream("localhost", 9999, '\n')

    val result: DataStream[String] = stream.filter(_.nonEmpty).map(new MyMapFunction)
      // 设置时间戳和水位线一定要在keyBy之前进行
      .assignTimestampsAndWatermarks(
        // 设置事件的最大延迟时间为5秒
        new MyWatermarkStrategy(Time.seconds(5)) // 水位线生成策略
          .withTimestampAssigner(new MySerializableTimestampAssigner) // 设置事件时间的获取方式
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new MyProcessWindowFunction)

    result.print()


    // 启动streaming程序
    env.execute("EventTimeExample")
  }

  /**
   * 自定义map函数，格式化数据
   */
  class MyMapFunction extends MapFunction[String, (String, Long)] {
    override def map(value: String): (String, Long) = {
      val arr = value.split(" ");
      (arr(0), arr(1).toLong * 1000)
    }
  }

  /**
   * 自定义的全窗口处理函数
   * 全窗口处理函数会在窗口关闭时执行
   */
  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    /**
     * 窗口关闭时要执行的函数
     *
     * @param key      本窗口的key
     * @param context  窗口上下文
     * @param elements 本窗口中的所有数据
     * @param out      输出
     */
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(new Timestamp(context.window.getStart) + "~" + new Timestamp(context.window.getEnd) + " 窗口中有 " + elements.size + " 个元素")
      for (elem <- elements) {
        out.collect(elem._1 + "---" + elem._2.toString)
      }
    }
  }

  /**
   * 自定义时间戳分配器，设置如何获取数据中的时间戳
   */
  class MySerializableTimestampAssigner extends SerializableTimestampAssigner[(String, Long)] {
    override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
      element._2
    }
  }

  /**
   * 设置watermark生成策略
   *
   * @param delay
   */
  class MyWatermarkStrategy(delay: Time) extends WatermarkStrategy[(String, Long)] {

    val milliseconds = delay.toMilliseconds

    /**
     * 创建watermark生成器。 程序启动时就会创建好
     *
     * @param context
     * @return
     */
    override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[(String, Long)] = {
      println("create WatermarkGenerator")
      new MyWatermarkGenerator(milliseconds)
    }
  }

  /**
   * 自定义watermark生成器
   *
   * @param delay 最大迟到时间
   */
  class MyWatermarkGenerator(delay: Long) extends WatermarkGenerator[(String, Long)] {
    var maxTimestamp: Long = Long.MinValue + delay;

    /**
     * 每个元素都会调用这个方法，如果我们想依赖每个元素生成一个水印，然后发射到下游(可选，就是看是否用output来收集水印)，我们可以实现这个方法.
     *
     * 建议数据量小时使用。
     * 当数据量较大时， 每条事件数据都创建一个水位线数据会影响性能。
     *
     * @param event  每条事件数据
     * @param eventTimestamp
     * @param output WatermarkOutput 如果需要每条数据生成一个watermark，则需要使用output.emitWatermark(new Watermark(maxTimestamp - delay))将创建的watermark插入到数据中
     */
    override def onEvent(event: (String, Long), eventTimestamp: Long, output: WatermarkOutput): Unit = {
      // 计算目前收到的最大事件时间戳
      maxTimestamp = maxTimestamp.max(eventTimestamp)
      println("当前事件数据 = " + event + " 当前数据的时间戳 = " + eventTimestamp + " 计算后的最大事件时间 = " + maxTimestamp)
    }

    /**
     * 如果数据量比较大的时候，我们每条数据都生成一个水印的话，会影响性能，所以这里还有一个周期性生成水印的方法。这个水印的
     *
     * 此函数默认定期200ms执行一次， 插入一条水位线数据。生成周期可以这样设置：env.getConfig().setAutoWatermarkInterval(5000L);
     *
     * @param output 将水位线数据插入到数据流中
     */
    override def onPeriodicEmit(output: WatermarkOutput): Unit = {
      // 插入水位线， 计算公式 水位线 = 当前最大的事件时间 - 允许迟到的时间
      println("插入水位线 = " + (maxTimestamp - delay))
      output.emitWatermark(new Watermark(maxTimestamp - delay))
    }
  }

}
