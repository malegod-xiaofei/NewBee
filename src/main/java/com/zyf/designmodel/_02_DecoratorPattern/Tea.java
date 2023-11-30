package com.zyf.designmodel._02_DecoratorPattern;

// 定义一个具体组件，表示茶
public class Tea extends Beverage {
    @Override
    public String getDescription() {
        return "茶";
    }

    @Override
    public double cost() {
        return 5.0;
    }
}
