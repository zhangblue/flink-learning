package com.zhangdi.flink.java.apitest.windows;

import com.zhangdi.flink.java.apitest.commons.function.PageFromMapFunction;
import com.zhangdi.flink.java.apitest.commons.watermark.PageFromSerializableTimestampAssigner;
import com.zhangdi.flink.java.apitest.commons.watermark.PageFromWatermarkStrategy;
import com.zhangdi.flink.java.apitest.commons.window.function.PageFromProcessWindowFunction;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author zhangdi
 * @description: 滚动窗口1
 * @date 2021/1/18 下午11:59
 * @since v1.0
 **/
public class TumblingWindowExample1 {


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

    executionEnvironment
        .addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties))
        .map(new PageFromMapFunction())
        .assignTimestampsAndWatermarks(
            new PageFromWatermarkStrategy(Duration.ofSeconds(5).toMillis())
                .withTimestampAssigner(new PageFromSerializableTimestampAssigner()))
        .keyBy(x -> x.getId())
        .window(TumblingEventTimeWindows.of(Time.seconds(3)))
        .process(new PageFromProcessWindowFunction())
        .print();

    try {
      executionEnvironment.execute("TumblingWindowExample1");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
