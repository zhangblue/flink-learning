package com.zhangdi.flink.java.apitest.model;

/**
 * @author zhangdi
 * @description: apache 日志源数据
 * @date 2021/1/15 下午5:45
 * @since v1.0
 **/
public class ApacheLogEventSource {

  private String ip;
  private String userId;
  private Long time;
  private String action;
  private String url;

  public ApacheLogEventSource(String ip, String userId, Long time, String action,
      String url) {
    this.ip = ip;
    this.userId = userId;
    this.time = time;
    this.action = action;
    this.url = url;
  }

  public String getIp() {
    return ip;
  }

  public String getUserId() {
    return userId;
  }

  public Long getTime() {
    return time;
  }

  public String getAction() {
    return action;
  }

  public String getUrl() {
    return url;
  }
}
