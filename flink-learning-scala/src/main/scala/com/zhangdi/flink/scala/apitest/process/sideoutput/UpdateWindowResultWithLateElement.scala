package com.zhangdi.flink.scala.apitest.process.sideoutput

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 使用迟到元素更新窗口计算结果
 *
 * @author di.zhang
 * @date 2020/8/30
 * @time 18:58
 **/
object UpdateWindowResultWithLateElement {
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
      .assignTimestampsAndWatermarks(
        new MyWatermarkStrategy(Time.seconds(5)) // 设置数据延迟时间5秒
          .withTimestampAssigner(new MySerializableTimestampAssigner))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5)) // 设置窗口长度为5秒
      .allowedLateness(Time.seconds(5)) // 设置窗口等待迟到数据的时间
      .process(new UpdateWindowResult)

    dataStream.print()


    env.execute()
  }

  class MyWatermarkStrategy(delayed: Time) extends WatermarkStrategy[(String, Long)] {
    val milliseconds = delayed.toMilliseconds

    override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[(String, Long)] = {
      new MyWatermarkGenerator(milliseconds);
    }
  }

  class MyWatermarkGenerator(toMilliseconds: Long) extends WatermarkGenerator[(String, Long)] {
    var maxTimestamp = Long.MinValue + toMilliseconds

    override def onEvent(event: (String, Long), eventTimestamp: Long, output: WatermarkOutput): Unit = {
      maxTimestamp = maxTimestamp.max(eventTimestamp)
      output.emitWatermark(new Watermark(maxTimestamp - toMilliseconds))
    }

    override def onPeriodicEmit(output: WatermarkOutput): Unit = {

    }
  }

  class MySerializableTimestampAssigner extends SerializableTimestampAssigner[(String, Long)] {
    override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
  }

  class UpdateWindowResult extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

    /**
     * 当水位线到达窗口结束时间时，触发调用此函数
     *
     * 因为设置了`.allowedLateness(Time.seconds(5))`参数。所以窗口不会立刻关闭，会等待后续迟到数据5秒后再关闭
     *
     * @param key
     * @param context
     * @param elements
     * @param out
     */
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      //设置窗口状态信息。窗口之间是隔离的，只有当前窗口可见
      val isUpdate = context.windowState.getState(
        new ValueStateDescriptor[Boolean]("update", Types.of[Boolean])
      )

      if (!isUpdate.value()) {
        //当前水位线超过窗口结束时间，第一次调用
        out.collect("窗口第一次进行求值，元素数量共有 " + elements.size + " 个！当前水位线为：" + context.currentWatermark)
        isUpdate.update(true)
      } else {
        out.collect("收到迟到数据。 更新的元素数量为:" + elements.size + " 个！当前水位线为：" + context.currentWatermark)
      }
    }
  }

}
