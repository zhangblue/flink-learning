package com.zhangdi.flink.java.apitest.windows;

import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    String brokers = parameterTool.get("broker-list");
    String topic = parameterTool.get("topic");
    String groupId = parameterTool.get("group-id");

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", brokers);
    properties.setProperty("group.id", groupId);

    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();
    executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStreamSource<String> kafkaDataStream = executionEnvironment
        .addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties));

    SingleOutputStreamOperator<Tuple3<String, Long, Integer>> map = kafkaDataStream
        .map(new SessionWindowTimeMapFunction());
    KeyedStream<Tuple3<String, Long, Integer>, String> tuple3StringKeyedStream = map
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(
                Duration.ofSeconds(1L)) //设置watermark时间为1秒
                .withTimestampAssigner((element, recordTimestamp) -> element.f1)
                .withIdleness(Duration.ofMinutes(1)) //标记空闲数据源 ：https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/event_timestamps_watermarks.html
        ).keyBy((KeySelector<Tuple3<String, Long, Integer>, String>) value -> value.f0);

    tuple3StringKeyedStream.print("textKey:");

    SingleOutputStreamOperator<String> aggregate = tuple3StringKeyedStream
        .window(EventTimeSessionWindows
            .withGap(Time.seconds(5L))) //设置session window的超时时间为5秒
        .aggregate(new SessionWindowTimeAggregate(), new SessionWindowWindowFunction());

    aggregate.print("value = ");

    executionEnvironment.execute("session window function");
  }


  private static class SessionWindowTimeMapFunction implements
      MapFunction<String, Tuple3<String, Long, Integer>> {

    @Override
    public Tuple3<String, Long, Integer> map(String value) throws Exception {
      String[] fields = value.split(",");
      return Tuple3.of(fields[0], Long.valueOf(fields[1]), 1);
    }
  }

  private static class SessionWindowTimeAggregate implements
      AggregateFunction<Tuple3<String, Long, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> createAccumulator() {
      return Tuple2.of(null, 0);
    }

    @Override
    public Tuple2<String, Integer> add(Tuple3<String, Long, Integer> value,
        Tuple2<String, Integer> accumulator) {
      return Tuple2.of(value.f0, accumulator.f1 + 1);
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
      return accumulator;
    }

    /**
     * 此函数用于将两个窗口中进行合并。只有会话窗口需要实现此函数，如果为非会话窗口，次函数可以直接return null
     * <p>
     * 具体请参见：https://blog.csdn.net/nazeniwaresakini/article/details/108576534
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
      return Tuple2.of(a.f0, a.f1 + b.f1);
    }
  }

  private static class SessionWindowWindowFunction implements
      WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input,
        Collector<String> out) throws Exception {

      out.collect("用户 " + s + " 在时间范围内 " + window.getStart() + "----" + window.getEnd() + " 访问了 "
          + input.iterator().next().f1 + " 次!");
    }
  }

}




