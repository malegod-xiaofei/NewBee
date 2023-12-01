package com.zyf.designmodel.Pattern02_Decorator;

// 定义一个抽象组件，表示饮料
public abstract class Beverage {
    // 返回饮料的描述
    public abstract String getDescription();
    // 返回饮料的价格
    public abstract double cost();
}