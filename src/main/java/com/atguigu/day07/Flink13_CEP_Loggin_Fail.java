package com.atguigu.day07;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink13_CEP_Loggin_Fail {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件中读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/LoginLog.csv");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<LoginEvent> loginEventDStream = streamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new LoginEvent(Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000;
                            }
                        })
        );

        //4.对用户id进行keyBy操作
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDStream.keyBy(r -> r.getUserId());

        //用户2秒内连续两次及以上登录失败则判定为恶意登录。
        //TODO 5.定义模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("begin")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                //两次及以上
                .timesOrMore(2)
                //连续-》严格连续
                .consecutive()
                //用户两秒内 -》 超时时间
                .within(Time.seconds(2));

        //TODO 6.将流作用于模式上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.将符合规则的数据输出到控制台
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();
    }
}
