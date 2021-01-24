package com.zhangdi.flink.java.apitest.cep;

import com.zhangdi.flink.java.apitest.model.OrderEvent;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

/**
 * @author zhangdi
 * @description: 订单条件
 * @date 2021/1/24 上午1:41
 * @since v1.0
 **/
public class OrderEventIterativeCondition extends IterativeCondition<OrderEvent> {

  private String eventType;

  public OrderEventIterativeCondition(String eventType) {
    this.eventType = eventType;
  }

  @Override
  public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
    return value.getEventType().equals(eventType);
  }
}
