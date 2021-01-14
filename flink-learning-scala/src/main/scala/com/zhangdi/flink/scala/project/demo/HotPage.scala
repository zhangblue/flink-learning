package com.zhangdi.flink.scala.project.demo

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.zhangdi.flink.scala.project.demo.HotPage.{ApacheLogEvent, UrlViewCount}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @description:
 * 需求：每隔5秒统计最近5分钟热门页面
 * <p>
 * 需求分析：滑动窗口，窗口大小5分钟，滑动步长5秒钟，计算Top N
 * <p>
 * 程序步骤：
 * 1.读取数据
 * 2.添加水位线(10秒)
 * 3.按照URL分组
 * 4.统计窗口数据
 * 5.根据窗口分组
 * 6.窗口数据排序
 * 7.打印输出
 * @author zhangdi
 * @date 2021/1/13 下午11:53
 * @since v1。0
 **/
object HotPage {

  /**
   * 输入数据样例类
   *
   * @param ip     访问的ip地址
   * @param userId 访问的用户id
   * @param time   访问时间
   * @param action 访问方式 (POST/GET)
   * @param url    访问地址
   */
  case class ApacheLogEvent(ip: String, userId: String, time: Long, action: String, url: String)

  /**
   * 窗口聚合返回样例类
   *
   * @param url       访问的url
   * @param windowEnd 所属窗口结束时间
   * @param count     窗口中的点击次数
   */
  case class UrlViewCount(url: String, windowEnd: Long, count: Long)

  def main(args: Array[String]): Unit = {

    val sourceFilePath: String = "/Users/zhangdi/work/workspace/github/myself/flink-learning/flink-learning-scala/src/main/resources/data2.log"
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    environment.readTextFile(sourceFilePath) //读取源文件
      .map(string2ApacheLogEvent(_)) //将数据进行解析
      .assignTimestampsAndWatermarks(new HotPageEventTimeExtractor(Time.seconds(10)).withTimestampAssigner(new EventTimestampAssigner)) //设置水位，允许数据迟到10秒
      .keyBy(new MyKeySelector) //key分配器
      .timeWindow(Time.minutes(5), Time.seconds(5)) //开滑动窗口，窗口大小5分钟，滑动步长5秒
      .aggregate(new PageCountAgg(), new PageWindowResult) //窗口URL进行统计
      .keyBy(_.windowEnd) //按照窗口结束时间进行汇总
      .process(new TopNHotPage(5))
      .print()


    environment.execute("hot page")


  }

  def string2ApacheLogEvent(line: String): ApacheLogEvent = {
    val fields = line.split(" ")
    val dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    val timeStamp = dateFormat.parse(fields(3).trim).getTime
    ApacheLogEvent(fields(0).trim, fields(1).trim, timeStamp, fields(5).trim, fields(6).trim)
  }
}

/**
 * 窗口聚合函数
 */
class PageWindowResult extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

/**
 * 窗口内聚合函数
 */
class PageCountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
  /**
   * 初始化迭代器
   *
   * @return
   */
  override def createAccumulator(): Long = 0L

  /**
   * 每条数据的聚合方式
   *
   * @param value
   * @param accumulator
   * @return
   */
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  /**
   * 两个窗口的累加器合并函数
   *
   * @param a
   * @param b
   * @return
   */
  override def merge(a: Long, b: Long): Long = a + b

  /**
   * 窗口关闭时执行的函数，返回聚合结果
   *
   * @param accumulator
   * @return
   */
  override def getResult(accumulator: Long): Long = accumulator


}


/**
 * 定义事件时间提取方式
 */
class EventTimestampAssigner extends SerializableTimestampAssigner[ApacheLogEvent] {
  override def extractTimestamp(element: ApacheLogEvent, recordTimestamp: Long): Long = {
    element.time
  }
}

/**
 * 自定义watermark注册器
 */
class HotPageEventTimeExtractor(delay: Time) extends WatermarkStrategy[ApacheLogEvent] {
  //迟到时间
  val milliseconds = delay.toMilliseconds

  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[ApacheLogEvent] = {
    new MyWatermarkGenerator(milliseconds)
  }
}

/**
 * 自定义watermark注册器
 */
class MyWatermarkGenerator(delay: Long) extends WatermarkGenerator[ApacheLogEvent] {
  var maxTimestamp: Long = Long.MinValue + delay;

  /**
   * 此函数为每条数据都有一个checkpoint
   *
   * @param event
   * @param eventTimestamp
   * @param output
   */
  override def onEvent(event: ApacheLogEvent, eventTimestamp: Long, output: WatermarkOutput): Unit = {
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
 * key分配器
 */
class MyKeySelector extends KeySelector[ApacheLogEvent, String] {
  override def getKey(value: ApacheLogEvent): String = {
    value.url
  }
}

/**
 * 计算top n
 *
 * @param topSize
 */
class TopNHotPage(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  //申明一个state，里面存储URL和对应出现的次数
  //TODO 这个地方用ListState也可以
  lazy val urlState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("url-state-count", classOf[String], classOf[Long]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlState.put(value.url, value.count)
    //注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  /**
   * 定时器
   *
   * @param timestamp
   * @param ctx
   * @param out
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlViews: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]

    val iter = urlState.entries().iterator()
    while (iter.hasNext) {
      val entity = iter.next()
      allUrlViews += ((entity.getKey, entity.getValue))
    }
    //清空status
    urlState.clear()
    //使用降序排列，求topN
    val sortedUrlView: ListBuffer[(String, Long)] = allUrlViews.sortWith(_._2 > _._2).take(topSize)

    val result = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    sortedUrlView.foreach(view => {
      result.append("URL:").append(view._1)
        .append(" 访问量：").append(view._2).append("\n")
    })
    result.append("===================")

    out.collect(result.toString())
  }
}
