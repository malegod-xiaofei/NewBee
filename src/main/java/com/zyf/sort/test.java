package com.zyf.sort;

import groovy.lang.Tuple;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author Malegod_xiaofei
 * @create 2024-04-17-16:41
 */
public class test {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setAppName("test").setMaster("local[*]");
        JavaSparkContext spark = new JavaSparkContext(sc);
        // 定义一个int数组
        int[] ints = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5};

        // 使用SparkContext的parallelize方法将数组转换为RDD
        JavaRDD<Integer> rdd = spark.parallelize(Arrays.asList(ArrayUtils.toObject(ints)), 2);

        JavaRDD<Integer> distinctRDD = rdd.distinct();

        distinctRDD.collect().forEach(x -> System.out.println(x));
    }
}
