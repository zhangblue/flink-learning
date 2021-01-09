package com.zhangdi.flink.scala.transformation

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.SensorSourceFromRandom
import org.apache.flink.streaming.api.scala._

/**
 * Union 将多个相同类型的DataStream流合并成一条流
 *
 * @author di.zhang
 * @date 2020/8/20
 * @time 21:23
 **/
object UnionExample {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val beijing: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom).filter(t => t.id.equals("sensor_4"))
    val guangzhou: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom).filter(t => t.id.equals("sensor_1"))
    val shenzhen: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom).filter(t => t.id.equals("sensor_3"))

    val unionStream: DataStream[SensorReading] = beijing.union(guangzhou, shenzhen)
    unionStream.print()

    env.execute("union example")
  }

}
