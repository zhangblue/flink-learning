package com.zhangdi.flink.java.api.test.stream.test.windows.function;

import com.zhangdi.flink.java.api.test.stream.test.model.SensorReading;
import com.zhangdi.flink.java.api.test.stream.test.source.SensorSourceFromRandom;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhangdi
 * @description: reduce 增量聚合函数
 * @date 2021/1/25 下午5:43
 * @since v1。0
 **/
public class ReduceWindowFunctionExample1 {

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
            return value1.getTemperature() > value2.getTemperature() ? value1 : value2;
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
