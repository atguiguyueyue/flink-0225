package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        //3.将数据转为JavaBean,返回嵌套的Tuple元组
        SingleOutputStreamOperator<Tuple2<Tuple2<String, Long>, Long>> map = streamSource.map(new MapFunction<String, Tuple2<Tuple2<String, Long>, Long>>() {
            @Override
            public Tuple2<Tuple2<String, Long>, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                AdsClickLog adsClickLog = new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );
                return Tuple2.of(Tuple2.of(adsClickLog.getProvince(), adsClickLog.getAdId()), 1L);
            }
        });

        //4.将数据聚和到一块
        KeyedStream<Tuple2<Tuple2<String, Long>, Long>, Tuple> keyedStream = map.keyBy(0);

        //5.累加计算
        keyedStream.sum(1).print();

        env.execute();
    }
}
