package com.zhangdi.flink.scala.transformation

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.random.SensorSourceFromRandom
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * 简单的connect coMap demo
 *
 * @author di.zhang
 * @date 2020/8/20
 * @time 21:51
 **/
object SimpleCoMapExample {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val beijing: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom).filter(t => t.id.equals("sensor_1"))
    val guangzhou: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom).filter(t => t.id.equals("sensor_1"))
    val connectedStream: ConnectedStreams[SensorReading, SensorReading] = beijing.keyBy(new MyKeySelector).connect(guangzhou.keyBy(new MyKeySelector))
    val coMaped: DataStream[String] = connectedStream.map(new MyCoMapFunction)
    coMaped.print()

    env.execute("simple coMap example")
  }

  class MyKeySelector extends KeySelector[SensorReading, String] {
    override def getKey(value: SensorReading): String = value.id
  }

  class MyCoMapFunction extends CoMapFunction[SensorReading, SensorReading, String] {
    override def map1(value: SensorReading): String = {
      "北京 " + value.id + " 的温度是 " + value.temperature
    }

    override def map2(value: SensorReading): String = "广州 " + value.id + " 的时间是 " + value.timestamp
  }

}
