package com.zyf.sort;

/**
 * @author Malegod_xiaofei
 * @create 2024-04-17-17:05
 */
public class test1 {
    public static void main(String[] args) {
        int[] data = {7, 1, 4, 3, 9, 5, 6, 8, 2, 10};
        int index = 0;
        while (index < data.length - 1) {
            for (int i = 0; i < data.length - 1 - index; i++) {
              int temp = 0;
              if (data[i] > data[i + 1]) {
                  temp = data[i];
                  data[i] = data[i + 1];
                  data[i + 1] = temp;
              }
            }
            index++;
        }
    }
}
