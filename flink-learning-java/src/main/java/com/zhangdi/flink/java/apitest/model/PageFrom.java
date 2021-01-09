package com.zhangdi.flink.java.apitest.model;

/**
 * @author zhangdi
 * @description: 数据来源
 * @date 2021/1/8 下午7:49
 * @since v1.0
 **/
public class PageFrom {

  private String id;
  private String from;
  private Long time;

  public PageFrom(String id, String from, Long time) {
    this.id = id;
    this.from = from;
    this.time = time;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getFrom() {
    return from;
  }

  public void setFrom(String from) {
    this.from = from;
  }

  public Long getTime() {
    return time;
  }

  public void setTime(Long time) {
    this.time = time;
  }
}
