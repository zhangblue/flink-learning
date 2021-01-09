package com.zhangdi.flink.scala.process.function

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.random.SensorSourceFromRandom
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 需求描述：
 * <p>
 * 监控传感器温度值，如果某个传感器温度值在3秒中内连续升高，则发出一条告警信息。
 *
 * 需要实现KeyedProcessFunction 接口
 * <p>
 *   1. processElement： 定义计算告警定时器何时会注册，或者销毁
 *   2. onTimer： 定义告警定时器的执行逻辑
 *
 * @author di.zhang
 * @date 2020/8/30
 * @time 12:33
 **/
object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val alertStream: DataStream[String] = env.addSource(new SensorSourceFromRandom)
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)

    alertStream.print()


    env.execute()
  }

  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {

    /**
     * 初始化一个状态变量，用来保存最近一次的温度值
     * lazy 懒加载。当执行process算子时，才会进行初始化
     * 这里使用状态变量的方式是为了可以讲数据保存在检查点，当程序重启时可以从最近的检查点中进行恢复
     * 状态变量只会被初始化一次，运行程序时，如果没有这个状态变量，就会初始化一个，如果有直接读取即可(.value())
     * 默认值为0.0
     */
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
    )

    /**
     * 用于保存报警定时器的时间戳，默认值是0L
     */
    lazy val timerTs = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("ts", Types.of[Long])
    )

    /**
     * 每条数据的处理逻辑
     *
     * <p>
     *   1. 上一次的温度数据
     *   2. 将本次的温度数据写入到温度状态变量中
     *   3. 如果本次的温度值小于上次的温度值，或者本次的温度为第一条温度时，清空告警定时器，清空告警定时器状态变量
     *   4. 如果本次的温度值大于上次的温度值，并且之前也没有创建过告警定时器时，则要创建一个告警定时器，告警定时器的触发时间为当前系统时间+3秒
     *
     * @param value
     * @param ctx
     * @param out
     */
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      //获取最近一次的温度，需要使用`.value()`方法, 如果没有值则默认为0.0
      val prevTemp = lastTemp.value()
      //将来的温度值更新到lastTemp状态变量中，使用update方法
      lastTemp.update(value.temperature)
      // 从报警定时器状态变量中获取定时器时间戳
      val currentTimer = timerTs.value()

      if (prevTemp == 0.0 || value.temperature < prevTemp) {
        // 如果来的温度是第一条文件，或者来的温度小于上一次的温度。
        //删除报警定时器
        ctx.timerService().deleteProcessingTimeTimer(currentTimer)
        //清空保存定时器时间戳的变量，使用clear方法
        timerTs.clear()
      } else if (value.temperature >= prevTemp && currentTimer == 0L) {
        //如果当前数据的温度大于上一次的温度，并且当前没有注册报警定时器时。注册一个报警定时器
        //如何认定没有注册报警定时器呢？  因为告警定时器状态变量中的值为0，即currentTimer==0L, 所以认为没有注册报警定时器
        val ts = ctx.timerService().currentProcessingTime() + 3 * 1000
        ctx.timerService().registerProcessingTimeTimer(ts)
        //将报警定时器的时间戳保存在状态变量中
        timerTs.update(ts)
      }
    }

    /**
     * 告警定时器的执行逻辑
     *
     * <p>
     *   1. 输出告警信息
     *   2. 清空告警定时器的状态变量。
     *
     * @param timestamp
     * @param ctx
     * @param out
     */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("传感器id: " + ctx.getCurrentKey + " 的温度连续上升!")
      //清空告警定时器状态保存信息
      timerTs.clear()
    }
  }

}
