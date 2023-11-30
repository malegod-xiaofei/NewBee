package com.zyf.designmodel._02_DecoratorPattern;

// 定义一个具体装饰者，表示奶油
public class Cream extends CondimentDecorator {

    public Cream(Beverage beverage) {
        super(beverage);
    }

    // 返回奶油和饮料的价格
    @Override
    public double cost() {
        return beverage.cost() + 2.0;
    }
}
