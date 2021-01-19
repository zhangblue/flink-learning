package com.zhangdi.flink.java.apitest.aggregate;

import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @author zhangdi
 * @description: 水位线测试2 使用增量聚合+全窗口聚合的方式
 * @date 2021/1/18 下午12:17
 * @since v1.0
 **/
public class AggregateFunctionExample2 {

  public static void main(String[] args) {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    String brokers = parameterTool.get("broker-list");
    String topic = parameterTool.get("topic");
    String groupId = parameterTool.get("group-id");

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", brokers);
    properties.setProperty("group.id", groupId);

    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();
    //设置water的插入周期为1分钟
    executionEnvironment.getConfig().setAutoWatermarkInterval(60000);

    SingleOutputStreamOperator<String> aggregate = executionEnvironment
        .addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties))
        .map(new MyMapFunction())
        .assignTimestampsAndWatermarks(new MyWatermarkStrategy(Duration.ofSeconds(10).toMillis()).
            withTimestampAssigner(new MySerializableTimestampAssigner()))
        .keyBy(t -> t.f0)
        .window(
            TumblingEventTimeWindows.of(Time.seconds(5))
        )
        .aggregate(new MyAggregateFunction(), new MyProcessWindowFunction());

    aggregate.print();

    try {
      executionEnvironment.execute("WaterMarkExample1");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public static class MyProcessWindowFunction extends
      ProcessWindowFunction<String, String, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<String> elements, Collector<String> out)
        throws Exception {
      out.collect(
          "窗口关闭，窗口范围 " + context.window().getStart() + " - " + context.window().getEnd() + " "
              + elements
              .iterator().next());
    }
  }

  public static class MyMapFunction implements MapFunction<String, Tuple2<String, Long>> {

    @Override
    public Tuple2<String, Long> map(String value) throws Exception {
      String[] split = value.split(",");
      return Tuple2.of(split[0], Long.parseLong(split[1]));
    }
  }


  public static class MyAggregateFunction implements
      AggregateFunction<Tuple2<String, Long>, Tuple3<String, Long, Integer>, String> {

    /**
     * 初始化函数
     *
     * @return
     */
    @Override
    public Tuple3<String, Long, Integer> createAccumulator() {
      return Tuple3.<String, Long, Integer>of("", 0L, 0);
    }

    /**
     * 添加元素时的聚合函数
     *
     * @param value
     * @param accumulator
     * @return
     */
    @Override
    public Tuple3<String, Long, Integer> add(Tuple2<String, Long> value,
        Tuple3<String, Long, Integer> accumulator) {
      System.out.println("当前数据为 " + value.toString());
      return Tuple3.<String, Long, Integer>of(value.f0, value.f1, accumulator.f2 + 1);
    }

    /**
     * 窗口关闭时的调用
     *
     * @param accumulator
     * @return
     */
    @Override
    public String getResult(Tuple3<String, Long, Integer> accumulator) {
      System.out.println("调用了getResult函数");
      return "用户: " + accumulator.f0 + " 访问了: " + accumulator.f2;
    }

    /**
     * 两个窗口合并时调用。只有sessionTimeWindow才需要实现此函数
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public Tuple3<String, Long, Integer> merge(Tuple3<String, Long, Integer> a,
        Tuple3<String, Long, Integer> b) {
      return null;
    }
  }

  /**
   * 时间戳提取器
   */
  public static class MySerializableTimestampAssigner implements
      SerializableTimestampAssigner<Tuple2<String, Long>> {

    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
      return element.f1;
    }
  }


  /**
   * 水位线生成策略
   */
  public static class MyWatermarkStrategy implements WatermarkStrategy<Tuple2<String, Long>> {

    private long boundTs;

    public MyWatermarkStrategy(long boundTs) {
      this.boundTs = boundTs;
    }

    @Override
    public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(
        WatermarkGeneratorSupplier.Context context) {
      return new MyWatermarkGenerator(boundTs);
    }
  }

  /**
   * 水位线生成器
   */
  public static class MyWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>> {

    private long bound;

    public MyWatermarkGenerator(long bound) {
      this.bound = bound;
    }

    private long maxTs = Long.MIN_VALUE + bound;

    /**
     * 此方法每条数据都会被调用一次。
     * <p>
     * 用于计算最大的事件时间戳
     *
     * @param event
     * @param eventTimestamp
     * @param output
     */
    @Override
    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
      maxTs = Long.max(event.f1, maxTs);
    }

    /**
     * 周期性调用。周期由  executionEnvironment.getConfig().setAutoWatermarkInterval(60000);控制
     * <p>
     * 生成水位线
     *
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      output.emitWatermark(new Watermark(maxTs - bound));
    }
  }
}
