package com.zhangdi.flink.java.api.test.stream.test.cep;

import com.zhangdi.flink.java.api.test.stream.test.model.OrderEvent;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author zhangdi
 * @description: 订单超时检测
 * @date 2021/1/24 上午1:32
 * @since v1.0
 **/
public class CepExample2 {

  public static void main(String[] args) {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<OrderEvent> orderEventDataStreamSource = env
        .fromElements(
            new OrderEvent("order_1", "create", 2000L),
            new OrderEvent("order_2", "create", 3000L),
            new OrderEvent("order_2", "pay", 4000L)
        );

    KeyedStream<OrderEvent, String> orderEventStringKeyedStream = orderEventDataStreamSource
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(
                (SerializableTimestampAssigner<OrderEvent>) (element, recordTimestamp) -> element
                    .getEventTime()))
        .keyBy(x -> x.getOrderId());

    Pattern<OrderEvent, OrderEvent> payPattern = Pattern
        .<OrderEvent>begin("first").where(new OrderEventIterativeCondition("create"))
        .next("second").where(new OrderEventIterativeCondition("pay"))
        .within(Time.seconds(5));

    PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStringKeyedStream, payPattern);
    //用来标记-超时订单的信息。 超时订单的意思是， 只有create事件， 没有pay事件
    OutputTag<OrderEvent> orderTimeoutOutputTag = new OutputTag<>("timeout",
        TypeInformation.of(OrderEvent.class));

    SingleOutputStreamOperator<OrderEvent> select = patternStream
        .select(orderTimeoutOutputTag, new OrderTimeoutFunction("first"),
            new OrderEventPatternSelectFunction("second"));

    // 输出超时订单的流
    select.getSideOutput(orderTimeoutOutputTag).print("超时的订单为 : ");
    // 输出支付成功的流
    select.print("支付成功的订单为 : ");

    // execute program
    try {
      env.execute("Flink Streaming Java API Skeleton");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 处理超时的订单数据
   */
  private static class OrderTimeoutFunction implements
      PatternTimeoutFunction<OrderEvent, OrderEvent> {

    private String eventType;

    public OrderTimeoutFunction(String eventType) {
      this.eventType = eventType;
    }

    @Override
    public OrderEvent timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp)
        throws Exception {
      List<OrderEvent> orderEvents = pattern.get(eventType);
      OrderEvent next = orderEvents.iterator().next();
      return next;
    }
  }

  /**
   * 处理正常的事件
   */
  private static class OrderEventPatternSelectFunction implements
      PatternSelectFunction<OrderEvent, OrderEvent> {

    private String eventType;

    public OrderEventPatternSelectFunction(String eventType) {
      this.eventType = eventType;
    }

    @Override
    public OrderEvent select(Map<String, List<OrderEvent>> pattern) throws Exception {
      OrderEvent next = pattern.get(eventType).iterator().next();
      return next;
    }
  }

}
