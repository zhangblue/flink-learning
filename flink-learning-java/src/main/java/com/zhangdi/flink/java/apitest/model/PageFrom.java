package com.zhangdi.flink.java.apitest.model;

/**
 * @author zhangdi
 * @description: 数据来源
 * @date 2021/1/8 下午7:49
 * @since v1.0
 **/
public class PageFrom {

  private String id;
  private String ip;
  private String evenType;
  private Long time;

  public PageFrom(String id, String ip, String evenType, Long time) {
    this.id = id;
    this.ip = ip;
    this.evenType = evenType;
    this.time = time;
  }

  public String getId() {
    return id;
  }

  public String getIp() {
    return ip;
  }

  public String getEvenType() {
    return evenType;
  }

  public Long getTime() {
    return time;
  }

  @Override
  public String toString() {
    return "PageFrom{" +
        "id='" + id + '\'' +
        ", ip='" + ip + '\'' +
        ", evenType='" + evenType + '\'' +
        ", time=" + time +
        '}';
  }
}
