package com.zhangdi.flink.java.apitest.commons.function;

import com.zhangdi.flink.java.apitest.model.PageFrom;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author zhangdi
 * @description: String转Tuple2 map function
 * @date 2021/1/19 上午12:37
 * @since v1.0
 **/
public class PageFromMapFunction implements MapFunction<String, PageFrom> {

  @Override
  public PageFrom map(String value) throws Exception {
    String[] split = value.split(",");
    return new PageFrom(split[0], split[1], split[2], Long.parseLong(split[3]) * 1000);
  }
}
