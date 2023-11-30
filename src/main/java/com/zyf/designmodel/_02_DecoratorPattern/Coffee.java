package com.zyf.designmodel._02_DecoratorPattern;

// 定义一个具体组件，表示咖啡
public class Coffee extends Beverage {
    @Override
    public String getDescription() {
        return "咖啡";
    }

    @Override
    public double cost() {
        return 10.0;
    }
}
