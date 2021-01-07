package com.zhangdi.flink.source

import java.util.Calendar

import com.zhangdi.flink.model.SensorReading
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

/**
 * 自定义数据源，返回的数据范型为 {@link SensorReading}
 *
 * 无限生成随机的SensorReading数据。
 *
 * @author di.zhang
 * @date 2020/8/20
 * @time 18:20
 **/
class SensorSourceFromRandom extends RichParallelSourceFunction[SensorReading] {
  //表示数据源是否正常运行。
  var running: Boolean = true

  /**
   * 上下文参数用来发出数据
   *
   * @param ctx
   */
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()


    //使用高斯噪声产生随机温度值
    var curFTemp = (1 to 10).map(i => ("sensor_" + i, rand.nextGaussian() * 20))

    //产生无限数据流
    while (running) {
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
      //产生ms为单位的时间戳
      val curTime = Calendar.getInstance.getTimeInMillis
      //发送数据
      curFTemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))

      //每间隔1秒发送一条传感器数据
      Thread.sleep(1000)
    }
  }

  /**
   * 定义当取消flink任务时，需要关闭数据源
   */
  override def cancel(): Unit = running = false
}
