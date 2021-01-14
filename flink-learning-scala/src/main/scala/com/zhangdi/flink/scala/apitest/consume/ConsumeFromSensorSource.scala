package com.zhangdi.flink.scala.apitest.consume

import com.zhangdi.flink.scala.apitest.model.SensorReading
import com.zhangdi.flink.scala.apitest.source.random.SensorSourceFromRandom
import org.apache.flink.streaming.api.scala._

/**
 * 从随机的无限无限数据流中读取数据
 *
 * @author di.zhang
 * @date 2020/8/20
 * @time 18:43
 **/
object ConsumeFromSensorSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)

    stream.print()

    env.execute()
  }

}
