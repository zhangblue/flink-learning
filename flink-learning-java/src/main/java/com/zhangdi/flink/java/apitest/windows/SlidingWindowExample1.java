package com.zhangdi.flink.java.apitest.windows;

import com.zhangdi.flink.java.apitest.commons.function.PageFromMapFunction;
import com.zhangdi.flink.java.apitest.commons.watermark.PageFromTimestampAssignerSupplier;
import com.zhangdi.flink.java.apitest.commons.watermark.PageFromWatermarkStrategy;
import com.zhangdi.flink.java.apitest.commons.window.function.PageFromProcessWindowFunction;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author zhangdi
 * @description: 滑动窗口 窗口大小5秒，滑动步长2秒
 * @date 2021/1/19 下午12:01
 * @since v1.0
 **/
public class SlidingWindowExample1 {

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
    executionEnvironment.getConfig().setAutoWatermarkInterval(3 * 1000);
    executionEnvironment.setParallelism(1);

    executionEnvironment
        .addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties))
        .map(new PageFromMapFunction())
        .assignTimestampsAndWatermarks(
            new PageFromWatermarkStrategy(Duration.ofSeconds(0).toMillis())
                .withTimestampAssigner(new PageFromTimestampAssignerSupplier())
                .withIdleness(Duration.ofSeconds(2)))
        .keyBy(x -> x.getId())
        .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
        .process(new PageFromProcessWindowFunction())
        .print();

    try {
      executionEnvironment.execute("SlidingWindowExample1");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
