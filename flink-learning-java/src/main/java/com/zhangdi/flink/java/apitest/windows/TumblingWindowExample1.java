package com.zhangdi.flink.java.apitest.windows;

import com.zhangdi.flink.java.apitest.commons.function.PageFromMapFunction;
import com.zhangdi.flink.java.apitest.commons.watermark.PageFromSerializableTimestampAssigner;
import com.zhangdi.flink.java.apitest.commons.window.function.PageFromProcessWindowFunction;
import com.zhangdi.flink.java.apitest.model.PageFrom;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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

    String brokers = parameterTool.get("broker-list", "172.16.36.123:9092");
    String topic = parameterTool.get("topic", "test-topic-p2");
    String groupId = parameterTool.get("group-id", "test-p2");

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", brokers);
    properties.setProperty("group.id", groupId);

    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();
    executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    executionEnvironment.getConfig().setAutoWatermarkInterval(1 * 1000);

    SingleOutputStreamOperator<PageFrom> map = executionEnvironment
        .addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties)
            .setStartFromLatest())
        .map(new PageFromMapFunction()).setParallelism(2);
    map.print();

    SingleOutputStreamOperator<String> process = map.assignTimestampsAndWatermarks(
        WatermarkStrategy.<PageFrom>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner(new PageFromSerializableTimestampAssigner()))
        .keyBy(x -> x.getId())
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .process(new PageFromProcessWindowFunction()).setParallelism(3);

    process.print("success = ").setParallelism(1);

    try {
      System.out.println(executionEnvironment.getExecutionPlan());
      executionEnvironment.execute("TumblingWindowExample1");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
