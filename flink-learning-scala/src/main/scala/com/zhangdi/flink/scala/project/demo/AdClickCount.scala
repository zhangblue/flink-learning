package com.zhangdi.flink.scala.project.demo

import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @description:
 * 实时统计广告点击，生成黑名单
 * 同一天内，同一用户，点击统一广告的次数>100，认为是黑名单
 * <p>
 * 程序步骤：
 * 1.数据读取
 * 2.添加水位线
 * 3.用户ID，广告ID 进行分组
 * 4.统计一天之内点击超过100次的进入黑名单
 * 5.根据测数据流输出黑名单
 * @author zhangdi
 * @date 2021/1/14 下午4:50
 * @since ${since}
 **/
object AdClickCount {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceFile = "/Users/zhangdi/work/workspace/github/myself/flink-learning/flink-learning-scala/src/main/resources/data3.csv"

    val outputBlackListTag = new OutputTag[String]("blacklist")

    val adEventStream: DataStream[AdClientEvent] = env.readTextFile(sourceFile)
      .map(string2ClickEvent(_))
      .assignTimestampsAndWatermarks(new AdClickCountEventTimeExtractor(Time.seconds(10)))
      .keyBy(data => (data.userId, data.adId)) //分组(userid,adid)
      .process(new CountBlackListUser(100, outputBlackListTag))

    adEventStream.getSideOutput(outputBlackListTag).print()


    env.execute("AdClickCount")
  }


  def string2ClickEvent(line: String): AdClientEvent = {
    val dataArray = line.split(",")
    AdClientEvent(dataArray(0), dataArray(1), dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
  }


}

/**
 * 用户访问样例类
 *
 * @param userId   用户id
 * @param adId     广告id
 * @param province 省份
 * @param city     城市
 * @param time     时间
 */
case class AdClientEvent(userId: String, adId: String, province: String, city: String, time: Long)

/**
 * 定义事件时间提取方式
 */
class AdClickCountEventTimestampAssigner extends SerializableTimestampAssigner[AdClientEvent] {
  override def extractTimestamp(element: AdClientEvent, recordTimestamp: Long): Long = {
    element.time
  }
}

/**
 * 自定义watermark注册器
 */
class AdClickCountEventTimeExtractor(delay: Time) extends WatermarkStrategy[AdClientEvent] {
  //迟到时间
  val milliseconds = delay.toMilliseconds

  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[AdClientEvent] = {
    new AdClickCountWatermarkGenerator(milliseconds)
  }
}

/**
 * 自定义watermark注册器
 */
class AdClickCountWatermarkGenerator(delay: Long) extends WatermarkGenerator[AdClientEvent] {
  var maxTimestamp: Long = Long.MinValue + delay;

  /**
   * 此函数为每条数据都有一个checkpoint
   *
   * @param event
   * @param eventTimestamp
   * @param output
   */
  override def onEvent(event: AdClientEvent, eventTimestamp: Long, output: WatermarkOutput): Unit = {
    maxTimestamp = maxTimestamp.max(eventTimestamp)
  }

  /**
   * 如果是定时checkpoint时使用
   *
   * @param output
   */
  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
    output.emitWatermark(new Watermark(maxTimestamp - delay))
  }
}

/**
 * 分组处理函数
 *
 * @param maxCount
 * @param outputTag
 */
class CountBlackListUser(maxCount: Int, outputTag: OutputTag[String]) extends KeyedProcessFunction[(String, String), AdClientEvent, AdClientEvent] {

  //记录当前用户对当前广告的点击量
  lazy val clickCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-click-state", classOf[Long]))
  //保存是否发送过黑名单
  lazy val isSetBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent-black-list", classOf[Boolean]))

  //保存定时器触发的时间戳
  lazy val saveTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-time-state", classOf[Long]))

  override def processElement(value: AdClientEvent, ctx: KeyedProcessFunction[(String, String), AdClientEvent, AdClientEvent]#Context, out: Collector[AdClientEvent]): Unit = {
    val currentCount = clickCountState.value()
    if (currentCount == 0) {
      //计算时间
      val ts: Long = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
      saveTimerState.update(ts)
      //注册定时器
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    if (currentCount >= maxCount) {
      if (!isSetBlackList.value()) {
        isSetBlackList.update(true)
        ctx.output(outputTag, "用户" + value.userId + " 对广告：" + value.adId + " 点击 " + currentCount + " 次")
      }
      return
    }

    clickCountState.update(currentCount + 1)
    out.collect(value)
  }

  /**
   * 定时器
   *
   * @param timestamp
   * @param ctx
   * @param out
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(String, String), AdClientEvent, AdClientEvent]#OnTimerContext, out: Collector[AdClientEvent]): Unit = {
    if (timestamp == saveTimerState.value()) {
      //清空状态
      isSetBlackList.clear()
      clickCountState.clear()
      saveTimerState.clear()
    }
  }
}


