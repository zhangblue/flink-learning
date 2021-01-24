package com.zhangdi.flink.java.apitest.cep;

import com.zhangdi.flink.java.apitest.model.PageFrom;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

/**
 * @author zhangdi
 * @description: CEP判断条件
 * @date 2021/1/9 下午4:06
 * @since v1.0
 **/
public class PageFromCepIterativeCondition extends IterativeCondition<PageFrom> {

  private String action;

  public PageFromCepIterativeCondition(String action) {
    this.action = action;
  }


  @Override
  public boolean filter(PageFrom value, Context<PageFrom> ctx) throws Exception {
    return value.getEvenType().equals(action);
  }
}
