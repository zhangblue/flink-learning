package com.zhangdi.flink.state

import com.zhangdi.flink.model.SensorReading
import com.zhangdi.flink.source.SensorSourceFromRandom
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 列表状态变量 {@link }样例
 *
 * @author di.zhang
 * @date 2020/9/25
 * @time 15:21
 **/
object ListStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置每10秒更新一次检查点文件
    env.enableCheckpointing(10000L)
    //设置检查点存储目录
    env.setStateBackend(new FsStateBackend("file:///Users/zhangdi/work/workspace/github/myself/flink-tutorial-code/src/main/resources/checkpoint"))

    val stream = env.addSource(new SensorSourceFromRandom)
      .filter(_.id.equals("sensor_1"))
      .keyBy(_.id)
      .process(new MyKeyed)
    stream.print

    env.execute()


  }

  class MyKeyed extends KeyedProcessFunction[String, SensorReading, String] {
    lazy val listState = getRuntimeContext.getListState(
      new ListStateDescriptor[SensorReading]("list-state", Types.of[SensorReading])
    )

    lazy val timer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer-state", Types.of[Long])
    )

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      listState.add(value)
      if (timer.value() == 0) {
        val ts = ctx.timerService().currentProcessingTime() + 10 * 1000L
        ctx.timerService().registerProcessingTimeTimer(ts)
        timer.update(ts)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 隐试类型转换必须导入
      import scala.collection.JavaConversions._
      out.collect("当前时刻列表状态变量里面共有 " + listState.get().size + " 条数据")
      timer.clear()
    }
  }

}
