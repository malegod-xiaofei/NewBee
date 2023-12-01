package com.zyf.designmodel.Pattern02_Decorator;

// 定义一个抽象装饰者，表示调料
public abstract class CondimentDecorator extends Beverage {
    // 持有一个被装饰者的引用
    protected Beverage beverage;

    public CondimentDecorator(Beverage beverage) {
        this.beverage = beverage;
    }

    // 返回调料和饮料的描述
    @Override
    public String getDescription() {
        return beverage.getDescription() + " + " + this.getClass().getSimpleName();
    }
}
