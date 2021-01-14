package com.zhangdi.flink.scala.apitest.window.trigger

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 触发器
 *
 * 实现逻辑：只在整数秒和窗口闭合时触发窗口计算
 *
 * @author di.zhang
 * @date 2020/8/30
 * @time 22:13
 **/
object TriggerExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream: DataStream[String] = env.socketTextStream("192.168.247.104", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .trigger(new OneSecondIntervalTrigger)
      .process(new WindowCount)

    stream.print()


    env.execute()


  }

  /**
   * 自定义触发器
   *
   * 只在整数秒和窗口闭合时触发窗口计算
   */
  class OneSecondIntervalTrigger extends Trigger[(String, Long), TimeWindow] {
    /**
     * 每条数据都会调用此函数
     *
     * @param element
     * @param timestamp
     * @param window
     * @param ctx
     * @return
     */
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      //默认值为false。当第一条数据来的时候，会将firstSeen置为true
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      //当第一条数据来的时候，firstSeen.value()为true
      //仅对第一条数据注册定时器
      //此处的定时器指的是onEventTime 函数
      if (!firstSeen.value()) {
        println("第一条数据来了，当前水位线为：" + ctx.getCurrentWatermark)
        //计算得到整数秒。如果当前水位线为1234ms，那么 t = 1234 + (1000 - 1234 % 1000) = 2000ms
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        println("第一条数据来了后，注册的整数秒时间戳为：" + t)
        //在第一条数据的时间戳后的整数秒注册一个定时器
        ctx.registerEventTimeTimer(t)
        println("第一条数据来了后，注册的窗口结束时间触发器为：" + window.getEnd)
        //在窗口结束时间注册一个定时器
        ctx.registerEventTimeTimer(window.getEnd)
        // 修改状态标记
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      //因为此处使用的事件时间，所以此时此方法不需要执行，直接继续即可
      TriggerResult.CONTINUE
    }

    /**
     * 当水位线达到time时触发
     *
     * @param time
     * @param window
     * @param ctx
     * @return
     */
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      // 在onElement 函数中注册过窗口结束时间的定时器，
      println("当前水位线：" + ctx.getCurrentWatermark)
      if (time == window.getEnd) {
        println("触发窗口闭合触发器，当前窗口结束时间为 " + window.getEnd + ", 当前水位线为:" + ctx.getCurrentWatermark)
        // 在窗口闭合时，触发计算并清空窗口
        TriggerResult.FIRE_AND_PURGE
      } else {
        // 在整数秒时，
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        println("触发整数秒触发, 当前水位线为:" + ctx.getCurrentWatermark)
        // 保证t小于窗口时间。因为如果t大于窗口结束时间，则表示执行了一个已经关闭掉窗口的触发器，是错误的。
        if (t < window.getEnd) {
          println("注册整数秒触发器:" + t)
          ctx.registerEventTimeTimer(t)
        }
        //触发计算
        TriggerResult.FIRE
      }
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      // 清空标记状态
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }

  class WindowCount extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("窗口中有 " + elements.size + " 条数据！窗口结束时间为：" + context.window.getEnd)
    }
  }

}
