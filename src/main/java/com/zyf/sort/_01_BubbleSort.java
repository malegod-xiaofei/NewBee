package com.zyf.sort;

/**
 * @author Malegod_xiaofei
 * @create 2024-04-01-22:34
 * 冒泡排序 时间复杂度 O(n^2) 空间复杂度 O(1)
 */
public class _01_BubbleSort {
    public static void bubbleSort(int[] data) {
        System.out.println("开始排序");
        int arrayLength = data.length;
        for (int i = 0; i < arrayLength - 1; i++) {
            boolean flag = false;
            for (int j = 0; j < arrayLength - 1 - i; j++) {
                if (data[j] > data[j + 1]) {
                    int temp = data[j + 1];
                    data[j + 1] = data[j];
                    data[j] = temp;
                    flag = true;
                }
            }
            System.out.println(java.util.Arrays.toString(data));
            if (!flag)
                break;
        }
    }

    public static void main(String[] args) {
        int[] data = {9, -16, 21, 23, -30, -49, 21, 30, 30};
        System.out.println(" 排 序 之 前 ： \n" +
                java.util.Arrays.toString(data));
        bubbleSort(data);
        System.out.println(" 排 序 之 后 ： \n" +
                java.util.Arrays.toString(data));
    }
}
