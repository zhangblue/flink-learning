package com.zhangdi.flink.java.apitest.cep;

import com.zhangdi.flink.java.apitest.model.PageFrom;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

/**
 * @author zhangdi
 * @description: CEP判断条件
 * @date 2021/1/9 下午4:06
 * @since v1.0
 **/
public class MyCepCondition extends SimpleCondition<PageFrom> {

  private String from;

  public MyCepCondition(String from) {
    this.from = from;
  }

  @Override
  public boolean filter(PageFrom value) throws Exception {

    return value.getFrom().equals(from);
  }
}
