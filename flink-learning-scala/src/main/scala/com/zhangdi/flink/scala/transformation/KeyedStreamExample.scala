package com.zhangdi.flink.scala.transformation

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.SensorSourceFromRandom
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

/**
 * keyBy函数demo
 *
 * @author di.zhang
 * @date 2020/8/20
 * @time 20:49
 **/
object KeyedStreamExample {

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)

    /**
     * 此处注意， keyedStream的范型中第一个元素为值的类型，第二个元素为key的类型
     */
    val keyed: KeyedStream[SensorReading, String] = stream.keyBy(new MyKeySelector)

    //按照temperature 字段求最小值
    val min: DataStream[SensorReading] = keyed.min("temperature")
    min.print("min : ")


    env.execute("keyedStream example")
  }

  /**
   * 自定义key选择器。按照id字段进行分组
   */
  class MyKeySelector extends KeySelector[SensorReading, String] {
    override def getKey(value: SensorReading): String = value.id
  }

}
