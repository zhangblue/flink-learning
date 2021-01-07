package com.zhangdi.flink.transformation

import com.zhangdi.flink.model.SensorReading
import com.zhangdi.flink.source.SensorSourceFromRandom
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 复杂的 connect comap demo
 * 使用侧数据流的方式
 *
 * @author di.zhang
 * @date 2020/8/20
 * @time 22:07
 **/
object ComplexCoMapExample {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = env.addSource(new SensorSourceFromRandom)

    //创建侧数据流标记
    val outputTag: OutputTag[SensorReading] = OutputTag("temperature_exception")
    //调用侧数据流方法，将数据分成正常数据和异常数据
    val resultDataStream: DataStream[SensorReading] = stream.process(new MyProcessFunction(outputTag))

    //得到主输出流中的正常
    val mainDataStream: DataStream[(String, Double)] = resultDataStream.map(t => (t.id, t.temperature))
    //得到侧数据流中的异常数据
    val sideOutputStream: DataStream[(String, Long, Double)] = resultDataStream.getSideOutput(outputTag).map(t => (t.id, t.timestamp, t.temperature))

    //将两个流合并
    val connDataStream: ConnectedStreams[(String, Double), (String, Long, Double)] = mainDataStream.connect(sideOutputStream)

    val coMapResult: DataStream[String] = connDataStream.map(new MyCoMapFunction)

    coMapResult.print()


    env.execute("complex coMap example")
  }

  class MyKeySelector extends KeySelector[SensorReading, String] {
    override def getKey(value: SensorReading): String = value.id
  }

  class MyCoMapFunction extends CoMapFunction[(String, Double), (String, Long, Double), String] {
    override def map1(value: (String, Double)): String = "传感器 " + value._1 + " 温度正常 " + value._2

    override def map2(value: (String, Long, Double)): String = "传感器: " + value._1 + " 温度异常: " + value._3 + " 时间: " + value._2
  }

  /**
   * 自定义一个ProcessFunction子类
   *
   * @param outputTag 用来给侧输出流中的数据添加标签
   *
   */
  class MyProcessFunction(outputTag: OutputTag[SensorReading]) extends ProcessFunction[SensorReading, SensorReading] {

    /**
     * 每分析DataStream中的一个元素，下述方法就执行一次
     *
     * @param value 当前的元素
     * @param ctx   上下文信息，用于向侧输出流中写入数据
     * @param out   用于向主输出流中写入数据
     */
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 0) {
        //温度小于0认为是异常温度. 放入侧输出流中，同时对数据打个标记，方便后续获取
        ctx.output[SensorReading](outputTag, value)
      } else {
        //正常温度的数据
        out.collect(value)
      }
    }
  }

}
