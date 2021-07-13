package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_Socket {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 3.从Socket中获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        streamSource.print();

        //执行任务
        env.execute();
    }
}
