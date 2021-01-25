package com.zhangdi.flink.java.api.test.stream.test.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Descrition 滚动计数窗口 每3条数据为一个窗口， 计算一次
 * @Author zhangd
 * @Date 2021/1/17 19:37
 */
public class CountWindowExample1 {

  public static void main(String[] args) {
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
   final int windowSize = parameterTool.getInt("window-size");

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<Tuple2<String, String>> sourceDataStream = env.fromElements(
        Tuple2.of("a", "1"),
        Tuple2.of("a", "2"),
        Tuple2.of("a", "3"),
        Tuple2.of("a", "4"),
        Tuple2.of("a", "5"),
        Tuple2.of("a", "6"),
        Tuple2.of("b", "7"),
        Tuple2.of("b", "8"),
        Tuple2.of("b", "9"),
        Tuple2.of("b", "0")
    );

    sourceDataStream
        .keyBy(
            (KeySelector<Tuple2<String, String>, String>) element -> element.f0)
        .countWindow(windowSize)
        .aggregate(new AggregateFunction<Tuple2<String, String>, Tuple2<String,Integer>, String>() {
          @Override
          public Tuple2<String, Integer> createAccumulator() {
            return null;
          }

          @Override
          public Tuple2<String, Integer> add(Tuple2<String, String> stringStringTuple2,
              Tuple2<String, Integer> stringIntegerTuple2) {
            return null;
          }

          @Override
          public String getResult(Tuple2<String, Integer> stringIntegerTuple2) {
            return null;
          }

          @Override
          public Tuple2<String, Integer> merge(Tuple2<String, Integer> stringIntegerTuple2,
              Tuple2<String, Integer> acc1) {
            return null;
          }
        });


  }
}
