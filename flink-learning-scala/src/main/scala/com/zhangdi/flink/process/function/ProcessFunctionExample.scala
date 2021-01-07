package com.zhangdi.flink.process.function

import com.zhangdi.flink.model.SensorReading
import com.zhangdi.flink.source.SensorSourceFromRandom
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 测数据流
 *
 * 需求：
 * <p>
 * 将温度值小于0的数据放入测数据流中
 *
 * @author di.zhang
 * @date 2020/8/30
 * @time 14:18
 **/
object ProcessFunctionExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //定义侧输出流标签
    val outPutTag: OutputTag[String] = new OutputTag[String]("freezing-alarm")

    val stream: DataStream[SensorReading] = env
      .addSource(new SensorSourceFromRandom)
      .process(new MyProcessFunction(outPutTag)) //调用自定义ProcessFunction 函数，传入侧输出流标签，给侧输出流数据打标签

    //从主输出流中输出
    stream.print("主数据流：")
    //通过侧输出流标签，输出侧输出流的数据
    stream.getSideOutput(outPutTag).print("侧输出流：")

    env.execute()

  }

  /**
   * 自定义ProcessFunction 将温度值小于0的数据做告警输出
   *
   * @param outPutTag 侧输出流标签。用于后续获取侧输出流中的内容使用
   */
  class MyProcessFunction(outPutTag: OutputTag[String]) extends ProcessFunction[SensorReading, SensorReading] {
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 0) {
        ctx.output(outPutTag, s"告警: ${value.id} 的温度值异常！温度值为: ${value.temperature}")
      } else {
        out.collect(value)
      }
    }
  }

}
