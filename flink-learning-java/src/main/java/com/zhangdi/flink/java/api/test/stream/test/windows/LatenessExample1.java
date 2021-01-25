package com.zhangdi.flink.java.api.test.stream.test.windows;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhangdi
 * @description: 处理晚到的数据:侧输出流
 * @date 2021/1/25 下午6:19
 * @since v1.0
 **/
public class LatenessExample1 {

  public static void main(String[] args) {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);

    String brokers = parameterTool.get("broker-list", "172.16.36.123:9092");
    String topic = parameterTool.get("topic", "test-topic");
    String groupId = parameterTool.get("group-id", "test");

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", brokers);
    properties.setProperty("group.id", groupId);

    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();

    SingleOutputStreamOperator<Tuple2<String, Long>> tupleSource = executionEnvironment
        .addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties))
        .map(new MapFunction<String, Tuple2<String, Long>>() {
          @Override
          public Tuple2<String, Long> map(String s) throws Exception {
            String[] split = s.split(",");
            return Tuple2.<String, Long>of(split[0], Long.parseLong(split[1]));
          }
        });

    // 定义旁路输出标记
    OutputTag<Tuple2<String, Long>> lateTag = new OutputTag<Tuple2<String, Long>>("late") {
    };

    SingleOutputStreamOperator<String> process = tupleSource
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(
                Duration.ZERO) //设置没有watermarter
                .withTimestampAssigner((element, recordTimestamp) -> element.f1))
        .keyBy(x -> x.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5))) //设置滚动窗口大小5秒
        .sideOutputLateData(lateTag) //设置旁路输出标记
        .process(
            new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
              @Override
              public void process(String s, Context context,
                  Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                Iterator<Tuple2<String, Long>> iterator = elements.iterator();
                int i = 0;
                String value = "";
                while (iterator.hasNext()) {
                  Tuple2<String, Long> next = iterator.next();
                  System.out.println(next.f1);
                  i++;
                }

                out.collect(
                    "传感器 " + s + " 在窗口 " + new Timestamp(context.window().getStart()) + " - "
                        + new Timestamp(context.window()
                        .getEnd()) + " 之间共收到了 " + i + " 条数据, " + value + " watermark : "
                        + new Timestamp(
                        context.currentWatermark()) + "===" + context.currentWatermark());
              }
            });

    process.print("正常的数据为 : "); //匹配到的主流输出
    process.getSideOutput(lateTag).printToErr("超时的数据为 : "); //旁路输出迟到的数据

    try {
      executionEnvironment.execute("侧数据流");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
