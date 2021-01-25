package com.zhangdi.flink.java.api.test.stream.test.windows.function;

import com.zhangdi.flink.java.api.test.stream.test.model.SensorReading;
import com.zhangdi.flink.java.api.test.stream.test.source.SensorSourceFromRandom;
import java.sql.Timestamp;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhangdi
 * @description: reduce增量聚合函数
 * @date 2021/1/25 下午5:29
 * @since v1.0
 **/
public class ReduceWindowFunctionExample2 {

  public static void main(String[] args) {
    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();

    DataStreamSource<SensorReading> sensorReadingDataStreamSource = executionEnvironment
        .addSource(new SensorSourceFromRandom());

    SingleOutputStreamOperator<SensorReading> sensorReadingSingleOutputStreamOperator = sensorReadingDataStreamSource
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

    sensorReadingSingleOutputStreamOperator
        .filter((FilterFunction<SensorReading>) value -> value.getId().equals("sensor_1"))
        .keyBy(x -> x.getId())
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .reduce(new ReduceFunction<SensorReading>() {
          @Override
          public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
            System.out.println("收到数据， 计算最大值！");
            return value1.getTemperature() > value2.getTemperature() ? value1 : value2;
          }
        }, new WindowFunction<SensorReading, String, String, TimeWindow>() {
          @Override
          public void apply(String key, TimeWindow window, Iterable<SensorReading> input,
              Collector<String> out) throws Exception {

            out.collect(
                "传感器 " + key + " 在窗口 " + new Timestamp(window.getStart()) + " - "
                    + new Timestamp(window
                    .getEnd()) + " 之间最高的温度为 " + input.iterator().next().getTemperature());
          }
        })
        .print();

    try {
      executionEnvironment.execute("reduce window function example1");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
