package com.zhangdi.flink.java.api.test.stream.test.windows;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * @description: 处理晚到的数据:将迟到数据统一处理
 * @date 2021/1/25 下午6:19
 * @since v1.0
 **/
public class LatenessExample2 {

  public static void main(String[] args) {
    StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment
        .getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "172.16.36.123:9092");
    properties.setProperty("group.id", "test");

    SingleOutputStreamOperator<Tuple2<String, Long>> tuple2DataStreamSource = executionEnvironment
        .addSource(
            new FlinkKafkaConsumer<String>("test-topic", new SimpleStringSchema(), properties)).map(
            new MapFunction<String, Tuple2<String, Long>>() {
              @Override
              public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.<String, Long>of(split[0], Long.parseLong(split[1]));
              }
            });

    tuple2DataStreamSource
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(((element, recordTimestamp) -> element.f1)))
        .keyBy(x -> x.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .allowedLateness(Time.seconds(5)) // 窗口闭合之后， 等待迟到元素的时间
        .process(new UpdateWindowResult())
        .print();

    try {
      executionEnvironment.execute("处理迟到数据");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  private static class UpdateWindowResult extends
      ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {


    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements,
        Collector<String> out) throws Exception {

      //当第一次队窗口进行求值时， 也就是水位线超过窗口结束时间的时候， 会第一次调用process函数， 这时isUpdate为默认值false
      //窗口内初始化一个状态变量使用windowState， 只对当前窗口可见
      ValueState<Boolean> isUpdate = context.windowState().getState(
          new ValueStateDescriptor<Boolean>("update", Boolean.class)
      );

      if (isUpdate.value() == null || !isUpdate.value()) {
        //当水位线超过窗口结束时间时， 第一次调用
        int size = 0;
        Iterator<Tuple2<String, Long>> iterator = elements.iterator();
        while (iterator.hasNext()) {
          iterator.next();
          size++;
        }
        out.collect("窗口第一进行求值了， 元素数量共有 " + size + " 个");
        isUpdate.update(Boolean.TRUE);
      } else {
        //说明是第二次执行process函数
        int size = 0;
        Iterator<Tuple2<String, Long>> iterator = elements.iterator();
        while (iterator.hasNext()) {
          iterator.next();
          size++;
        }
        out.collect("迟到元素到来了！更新的元素数量为: " + size + " 个");
      }
    }
  }
}


