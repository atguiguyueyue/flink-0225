package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink08_CEP_Single_Loop_Consecutive {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> sensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        SingleOutputStreamOperator<WaterSensor> streamOperator = sensorDStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs()*1000;
                    }
                })
        );

        //TODO 定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                .where(new IterativeCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                //TODO 循环两次 （固定次数）
                .times(2) //默认是松散连续的
                //范围内次数
//                .times(1,3)
                //一次或多次
//                .oneOrMore()
                //两次及两次以上
//                .timesOrMore(2)
                //严格连续
//                .consecutive()
                //非确定的松散连续
                .allowCombinations();



//                .followedByAny("end")
//                .where(new SimpleCondition<WaterSensor>() {
//                    @Override
//                    public boolean filter(WaterSensor value) throws Exception {
//                        return "sensor_1".equals(value.getId());
//                    }
//                })
//               ;

        //TODO 将流作用于模式上
        PatternStream<WaterSensor> patternStream = CEP.pattern(streamOperator, pattern);

        //TODO 获取符合条件的数据
        patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();
    }
}
