package com.zyf.designmodel.Pattern02_Decorator;

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
