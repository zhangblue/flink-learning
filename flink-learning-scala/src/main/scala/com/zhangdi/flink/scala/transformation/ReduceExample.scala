package com.zhangdi.flink.scala.transformation

import com.zhangdi.flink.scala.model.SensorReading
import com.zhangdi.flink.scala.source.SensorSourceFromRandom
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

/**
 * reduce 函数demo
 *
 * @author di.zhang
 * @date 2020/8/20
 * @time 21:07
 **/
object ReduceExample {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)


    /**
     * 此处注意， keyedStream的范型中第一个元素为值的类型，第二个元素为key的类型
     */
    val keyed: KeyedStream[SensorReading, String] = stream.keyBy(new MyKeySelector)
    //将分组后的前后两个数据做汇聚操作
    val reduce: DataStream[SensorReading] = keyed.reduce(new MyReduceFunction)
    reduce.print("reduce : ")

    env.execute("reduce example")
  }

  /**
   * 按照id字段进行分组
   */
  class MyKeySelector extends KeySelector[SensorReading, String] {
    override def getKey(value: SensorReading): String = value.id
  }

  /**
   * 前后两个元素进行聚合， 得到一条SensorReading类型的聚合结果数据
   */
  class MyReduceFunction extends ReduceFunction[SensorReading] {
    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
      SensorReading(value1.id, 0L, value1.temperature.min(value2.temperature))
    }
  }

}
