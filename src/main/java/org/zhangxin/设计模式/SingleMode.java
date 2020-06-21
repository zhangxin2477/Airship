package org.zhangxin.设计模式;

/**
 * 单例模式
 */
public class SingleMode {
    private static volatile SingleMode singleMode;

    private SingleMode() {
    }

    // 1、双重检查
    public static SingleMode getInstance() {
        if (singleMode == null) {
            synchronized (SingleMode.class) {
                if (singleMode == null) {
                    singleMode = new SingleMode();
                }
            }
        }
        return singleMode;
    }

    //2、静态内部类
    public static SingleMode getInstance2() {
        return SingleModeInstance.instance;
    }

    private static class SingleModeInstance {
        private static final SingleMode instance = new SingleMode();
    }
}
