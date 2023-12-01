package com.zyf.designmodel.Pattern01_Singleton;

/**
 * @author Malegod_xiaofei
 * @create 2023-11-30-15:30
 */
public class LazyDoubleCheckSingleton {
    private LazyDoubleCheckSingleton() {
    }

    private volatile static LazyDoubleCheckSingleton instance;

    public static LazyDoubleCheckSingleton getInstance() {
        //确定是否需要阻塞
        if (instance == null) {
            // 线程安全：双重检查锁（同步代码块）
            synchronized (LazyDoubleCheckSingleton.class) {
                //确定是否需要创建实例
                if (instance == null) {
                    //这里在多线程的情况下会出现指令重排的问题，所以对共有资源instance使用关键字volatile修饰
                    instance = new LazyDoubleCheckSingleton();
                }
            }
        }
        return instance;
    }
}