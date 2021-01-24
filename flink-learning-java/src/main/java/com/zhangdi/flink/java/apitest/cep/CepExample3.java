package com.zhangdi.flink.java.apitest.cep;

import com.zhangdi.flink.java.apitest.model.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhangdi
 * @description: 使用底层API，不使用CEP的方式实现超时订单检测
 * @date 2021/1/24 上午1:32
 * @since v1.0
 **/
public class CepExample3 {

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

    orderEventStringKeyedStream.process(new MatchFunction()).print();

    // execute program
    try {
      env.execute("Flink Streaming Java API Skeleton");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  private static class MatchFunction extends KeyedProcessFunction<String, OrderEvent, String> {

    ValueState<OrderEvent> orderEventValueState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
      orderEventValueState = getRuntimeContext().getState(
          new ValueStateDescriptor<OrderEvent>("saved order", OrderEvent.class)
      );
    }


    @Override
    public void processElement(OrderEvent value, Context ctx, Collector<String> out)
        throws Exception {

      if (value.getEventType().equals("create")) {
        //防止pay事件先到
        if (orderEventValueState.value() == null) {
          // 保存的是create事件
          orderEventValueState.update(value);
          // 注册一个定时器
          ctx.timerService().registerEventTimeTimer(value.getEventTime() + 5000L);
        }
      } else {
        // 保存的是pay事件
        out.collect("已经支付的订单是 " + value.getOrderId());
        orderEventValueState.update(value);
      }


    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
        throws Exception {

      OrderEvent savedOrder = orderEventValueState.value();
      if (savedOrder != null && savedOrder.getEventType().equals("create")) {
        out.collect("超时订单是 " + savedOrder.getOrderId());
      }
      orderEventValueState.clear();
    }
  }

}
