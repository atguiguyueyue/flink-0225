package com.atguigu.day03.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_Transform_Process {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3.使用map对数据进行转换，将读进来的数据封装成WaterSensor
        SingleOutputStreamOperator<WaterSensor> process = streamSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));

            }
        });

        KeyedStream<WaterSensor, Tuple> keyedStream = process.keyBy("id");

        //TODO 在keyBy之后使用process
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println(ctx.getCurrentKey());
                out.collect(new WaterSensor(value.getId() + "proceess", value.getTs() + System.currentTimeMillis(), value.getVc() + 1000));
            }
        }).print();

        env.execute();

    }

}
