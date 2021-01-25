package com.zhangdi.flink.java.api.test.stream.test.model;

import java.io.Serializable;

/**
 * @author zhangdi
 * @description: apache 日志源数据
 * @date 2021/1/15 下午5:45
 * @since v1.0
 **/
public class UserActionLogEventSource implements Serializable {

  private static final long serialVersionUID = 1L;

  private String userId;
  private Long time;
  private String url;

  public UserActionLogEventSource(String userId, Long time, String url) {
    this.userId = userId;
    this.time = time;
    this.url = url;
  }

  public String getUserId() {
    return userId;
  }

  public Long getTime() {
    return time;
  }

  public String getUrl() {
    return url;
  }
}
