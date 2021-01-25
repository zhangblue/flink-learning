package com.zhangdi.flink.java.api.test.stream.test.model;

/**
 * @author zhangdi
 * @description: 订单事件
 * @date 2021/1/24 上午1:33
 * @since v1.0
 **/
public class OrderEvent{



  private String orderId;
  private String eventType;
  private long eventTime;

  public OrderEvent(String orderId, String eventType, long eventTime) {
    this.orderId = orderId;
    this.eventType = eventType;
    this.eventTime = eventTime;
  }

  public String getOrderId() {
    return orderId;
  }

  public String getEventType() {
    return eventType;
  }

  public long getEventTime() {
    return eventTime;
  }

  @Override
  public String toString() {
    return "OrderEvent{" +
        "orderId='" + orderId + '\'' +
        ", eventType='" + eventType + '\'' +
        ", eventTime=" + eventTime +
        '}';
  }
}
