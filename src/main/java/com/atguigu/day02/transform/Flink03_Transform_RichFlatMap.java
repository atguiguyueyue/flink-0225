package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class Flink03_Transform_RichFlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

//        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
//        DataStreamSource<String> streamSource = env.readTextFile("input/waterSensor.txt");
        List<String> list = Arrays.asList("s1,1,1", "s2,2,2");
        DataStreamSource<String> streamSource = env.fromCollection(list);
        //TODO FlatMap
        streamSource.flatMap(new MyFlatMap()).print();

        env.execute();
    }

    //自定义一个类，来实现富函数的抽象类
    public static class MyFlatMap extends RichFlatMapFunction<String,String>{

        /**
         * 生命周期，会在启动时调用，每个并行实例调用一次
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open.....");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] words = value.split(",");
            System.out.println(getRuntimeContext().getTaskName());

            for (String word : words) {
                out.collect(word);
            }
        }

        /**
         * 生命周期程序结束时调用，每个并行实例调用一次,注意！！！！读文件时会调用两次
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close.....");
        }
    }
}
