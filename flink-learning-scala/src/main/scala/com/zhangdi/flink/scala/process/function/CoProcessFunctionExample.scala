package com.zhangdi.flink.scala.process.function

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.SensorSourceFromRandom
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 自定义 {@link CoProcessFunction} 实现类
 *
 * 需求：
 * <p>
 * 使用第二个流中的数据作为开关，开关控制第一条流中相同的key的数据输出的时间
 *
 * @author di.zhang
 * @date 2020/8/30
 * @time 15:20
 **/
object CoProcessFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //传感器数据流，无限流
    val stream1: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)

    //开关流。有限流
    val stream2: DataStream[(String, Long)] = env.fromElements(
      ("sensor_2", 5 * 1000), ("sensor_5", 3 * 1000)
    )
    stream1.connect(stream2) // 合流
      .keyBy(_.id, _._1) // 使用第一个流的.id字段与第二个流的._2字段进行分组
      .process(new MyCoProcessFunction)
      .print()
    env.execute()

  }

  /**
   *
   */
  class MyCoProcessFunction extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {

    /**
     * 定义开关状态信息。默认值为false
     */
    lazy val forwardingEnable = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("switch", Types.of[Boolean])
    )

    /**
     * 来自传感器流的数据，根据开关状态信息判断是否要想下游输出
     *
     * @param value
     * @param ctx
     * @param out
     */
    override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      // 如果开关是true，则允许数据流向下发送
      if (forwardingEnable.value()) {
        out.collect(value)
      }
    }

    /**
     * 来自开关流的数据。将开关状态信息标记为true，并且注册一个执行时间为 `currentProcessingTime + value._2`` 的定时器
     * @param value
     * @param ctx
     * @param out
     **/
    override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      // 打开开关
      forwardingEnable.update(true)
      // 计算定时器执行的时间。开关元祖的第二个值为放行时间。
      val ts = ctx.timerService().currentProcessingTime() + value._2
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    /**
     * 当定时器执行时，将开关状态信息清空
     *
     * @param timestamp
     * @param ctx
     * @param out
     */
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      forwardingEnable.clear()
    }
  }

}
