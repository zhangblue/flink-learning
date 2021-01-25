package com.zhangdi.flink.java.api.test.stream.test.windows.function;

import com.zhangdi.flink.java.api.test.stream.test.model.SensorReading;
import com.zhangdi.flink.java.api.test.stream.test.source.SensorSourceFromRandom;
import java.sql.Timestamp;
import java.util.Iterator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhangdi
 * @description: 全窗口计算函数
 * @date 2021/1/25 下午5:12
 * @since v1.0
 **/
public class ProcessWindowFunctionExample1 {


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
        .process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {
          @Override
          public void process(String key, Context context, Iterable<SensorReading> elements,
              Collector<String> out) throws Exception {
            Iterator<SensorReading> iterator = elements.iterator();
            int i = 0;
            while (iterator.hasNext()) {
              SensorReading next = iterator.next();
              i++;
            }

            out.collect(
                "传感器 " + key + " 在窗口 " + new Timestamp(context.window().getStart()) + " - "
                    + new Timestamp(context.window()
                    .getEnd()) + " 之间共收到了 " + i + " 条数据");
          }
        })
        .print();

    try {
      executionEnvironment.execute("process window function example1");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
