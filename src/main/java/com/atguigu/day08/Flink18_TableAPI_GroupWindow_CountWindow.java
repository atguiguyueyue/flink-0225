package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class Flink18_TableAPI_GroupWindow_CountWindow {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60),
                new WaterSensor("sensor_2", 6000L, 60),
                new WaterSensor("sensor_2", 6000L, 60),
                new WaterSensor("sensor_2", 6000L, 60),
                new WaterSensor("sensor_2", 6000L, 60),
                new WaterSensor("sensor_2", 6000L, 60));


        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.1将流转为表,并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensorStream,$("id"),$("ts"),$("vc"),$("pt").proctime());
//        Table table = tableEnv.fromDataStream(waterSensorStream,$("id"),$("ts").rowtime(),$("vc"));

        //TODO 3.查询表时开启一个基于元素个数的滑动窗口
        Table result = table
                .window(Slide.over(rowInterval(4L)).every(rowInterval(2L)).on($("pt")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("vc").sum());

        result.execute().print();


    }
}
