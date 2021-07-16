package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;


public class Flink03_Project_UV {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件中获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.将数据转为javaBean
        SingleOutputStreamOperator<UserBehavior> userBehaviorDStream = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                //将数据按照逗号切分
                String[] split = value.split(",");
                return new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));
            }
        });

        //4.将pv的数据过滤出来
        SingleOutputStreamOperator<UserBehavior> pvDataDStream = userBehaviorDStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        //5.将数据转为Tuple
        SingleOutputStreamOperator<Tuple2<String, Long>> pvToOneDStream = pvDataDStream.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv",value.getUserId());
            }
        });

        //6.将数据聚和起来
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = pvToOneDStream.keyBy(0);

        //7.累加计算
        keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String,Long>>() {

            HashSet<Long> hashSet = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                //TODO 里面来一条数据调用一次
                //1.去重
                hashSet.add(value.f1);

                //2.将结果数据发送给下游
                out.collect(Tuple2.of("uv",(long) hashSet.size()));
            }
        }).print();

        env.execute();
    }
}
