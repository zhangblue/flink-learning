package com.zhangdi.flink.java.api.test.stream.test.aviator;

import com.googlecode.aviator.AviatorEvaluator;
import com.zhangdi.flink.java.api.test.stream.test.model.PageFrom;

/**
 * @author zhangdi
 * @description: aviator测试
 * @date 2021/1/22 下午1:43
 * @since v1.0
 **/
public class JavaAviatorDemo {

  public static void main(String[] args) {
    PageFrom lo = (PageFrom)AviatorEvaluator.execute("new com.zhangdi.flink.java.apitest.model.PageFrom('a','b','c',100)");

    System.out.println(lo.toString());

  }


}
