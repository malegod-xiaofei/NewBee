package com.zyf.designmodel._02_DecoratorPattern;

// 定义一个具体装饰者，表示糖
public class Sugar extends CondimentDecorator {

    public Sugar(Beverage beverage) {
        super(beverage);
    }

    // 返回糖和饮料的价格
    @Override
    public double cost() {
        return beverage.cost() + 1.0;
    }
}
