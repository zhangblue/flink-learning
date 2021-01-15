package com.zhangdi.flink.java.apitest.windows;

import com.zhangdi.flink.java.apitest.model.ApacheLogEventSource;
import com.zhangdi.flink.java.apitest.source.SourceFromKafka;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @author zhangdi
 * @description: session window demo
 * @date 2021/1/15 下午5:01
 * @since v1.0
 **/
public class SessionWindowExample1 {

  public static void main(String[] args) {
    try {
      new SessionWindowExample1().doRun(ParameterTool.fromArgs(args));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void doRun(ParameterTool parameterTool) throws Exception {
    String brokers = parameterTool.get("broker-list");
    String topic = parameterTool.get("topic");
    String groupId = parameterTool.get("group-id");

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", brokers);
    properties.setProperty("group.id", groupId);

    FlinkKafkaConsumer<Tuple5<String, Integer, Long, String, String>> kafkaConsumer = SourceFromKafka
        .getKafkaConsumer(topic, properties);
    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();
    executionEnvironment.setParallelism(1);
    executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    SingleOutputStreamOperator<String> aggregate = executionEnvironment
        .addSource(kafkaConsumer)
        .map(new SessionWindowRichMapFunction())
        .assignTimestampsAndWatermarks(
            new SessionWindowWatermarkStrategy(Time.seconds(10))
                .withTimestampAssigner(new SessionWindowSerializableTimestampAssigner()))
        .keyBy(new ApacheLogEventSourceKeySelector())
        .window(EventTimeSessionWindows
            .withGap(org.apache.flink.streaming.api.windowing.time.Time.seconds(20)))
        .aggregate(new SessionWindowAggregateFunction(), new SessionWindowWindowFunction());

    aggregate.print("value = ");

    executionEnvironment.execute("session window function");


  }


  public static ApacheLogEventSource string2ApacheLogEvent(String line, SimpleDateFormat dateFormat)
      throws ParseException {
    String[] fields = line.split(" ");
    long time = dateFormat.parse(fields[3].trim()).getTime();
    return new ApacheLogEventSource(fields[0].trim(), fields[1], time, fields[5].trim(),
        fields[6].trim());
  }

  private class SessionWindowWatermarkStrategy implements WatermarkStrategy<ApacheLogEventSource> {

    private Time time;

    public SessionWindowWatermarkStrategy(Time time) {
      this.time = time;
    }

    @Override
    public WatermarkGenerator<ApacheLogEventSource> createWatermarkGenerator(
        WatermarkGeneratorSupplier.Context context) {
      return new SessionWindowWatermarkGenerator(time.toMilliseconds());
    }
  }


  /**
   * 水位线生成器
   */
  private class SessionWindowWatermarkGenerator implements
      WatermarkGenerator<ApacheLogEventSource>, Serializable {

    private long delay;
    private long maxTimestamp;

    public SessionWindowWatermarkGenerator(long delay) {
      this.delay = delay;
      maxTimestamp = Long.MIN_VALUE + delay;
    }

    /**
     * 每条数据都会调用
     * <p>
     * 计算最大时间戳
     *
     * @param event
     * @param eventTimestamp
     * @param output
     */
    @Override
    public void onEvent(ApacheLogEventSource event, long eventTimestamp, WatermarkOutput output) {
      maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    /**
     * 定时checkpoint调用
     *
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      output.emitWatermark(new Watermark(maxTimestamp - delay));
    }
  }

  private class SessionWindowSerializableTimestampAssigner implements
      SerializableTimestampAssigner<ApacheLogEventSource> {

    @Override
    public long extractTimestamp(ApacheLogEventSource element, long recordTimestamp) {
      return element.getTime();
    }
  }

  /**
   * 对象转换
   */
  private class SessionWindowRichMapFunction extends
      RichMapFunction<Tuple5<String, Integer, Long, String, String>, ApacheLogEventSource> {

    private SimpleDateFormat simpleDateFormat = null;

    @Override
    public void open(Configuration parameters) throws Exception {
      simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
    }

    @Override
    public ApacheLogEventSource map(Tuple5<String, Integer, Long, String, String> value)
        throws Exception {
      return SessionWindowExample1.string2ApacheLogEvent(value.f4, simpleDateFormat);
    }
  }

  /**
   * ApacheLogEventSource key 选择器
   */
  private class ApacheLogEventSourceKeySelector implements
      KeySelector<ApacheLogEventSource, String> {

    @Override
    public String getKey(ApacheLogEventSource value) throws Exception {
      return value.getUserId();
    }
  }

  private class SessionWindowAggregateFunction implements
      AggregateFunction<ApacheLogEventSource, Long, Long> {


    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long add(ApacheLogEventSource value, Long accumulator) {
      return accumulator++;
    }

    @Override
    public Long getResult(Long accumulator) {
      return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
      return a + b;
    }
  }

  private class SessionWindowWindowFunction implements
      WindowFunction<Long, String, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<String> out)
        throws Exception {
      out.collect("用户 " + s + " 在 " + window.getStart() + "-" + window.getEnd() + " 之前共访问了 " + input
          .iterator().next() + " 次！");

    }
  }

}




