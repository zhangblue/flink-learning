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
  private String from;
  private Long time;

  public PageFrom(String id, String ip, String from, Long time) {
    this.id = id;
    this.ip = ip;
    this.from = from;
    this.time = time;
  }

  public String getId() {
    return id;
  }

  public String getIp() {
    return ip;
  }

  public String getFrom() {
    return from;
  }

  public Long getTime() {
    return time;
  }

  @Override
  public String toString() {
    return "PageFrom{" +
        "id='" + id + '\'' +
        ", ip='" + ip + '\'' +
        ", from='" + from + '\'' +
        ", time=" + time +
        '}';
  }
}
