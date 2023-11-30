package com.zyf.designmodel._02_DecoratorPattern;

// 测试代码
public class Test {

    public static void main(String[] args) {
        // 创建一个咖啡对象
        Beverage coffee = new Coffee();
        System.out.println(coffee.getDescription() + " = " + coffee.cost());

        // 给咖啡加糖和奶油
        coffee = new Sugar(coffee);
        coffee = new Cream(coffee);
        System.out.println(coffee.getDescription() + " = " + coffee.cost());

        // 创建一个茶对象
        Beverage tea = new Tea();
        System.out.println(tea.getDescription() + " = " + tea.cost());

        // 给茶加糖
        tea = new Sugar(tea);
        System.out.println(tea.getDescription() + " = " + tea.cost());
    }
}
