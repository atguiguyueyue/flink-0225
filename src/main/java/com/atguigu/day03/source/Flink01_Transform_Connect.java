package com.atguigu.day03.source;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class Flink01_Transform_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据源
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5, 6);

        DataStreamSource<String> source2 = env.fromElements("a", "b", "c", "d", "e", "f");

        //TODO 3.使用Connect连接两条流
        ConnectedStreams<Integer, String> connect = source1.connect(source2);

        connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value + "map1";
            }

            @Override
            public String map2(String value) throws Exception {
                return "map2" + value;
            }
        }).print();

        env.execute();
    }
}
