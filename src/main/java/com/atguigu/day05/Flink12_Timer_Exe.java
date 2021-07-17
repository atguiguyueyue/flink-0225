package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink12_Timer_Exe {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.对数据进行处理，封装成WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        //监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            //声明一个变量用来保存上次水位值
            private Integer lastVc = Integer.MIN_VALUE;

            //声明一个变量用来初始化定时器（保存定时器时间）
            private Long timer = Long.MIN_VALUE;


            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //1.判断当前水位值是否高于上次水位值
                if (value.getVc() > lastVc) {
                    //如果没有注册定时器那么注册一个定时器
                    if (timer == Long.MIN_VALUE) {
                        System.out.println("注册定时器");
                        //给保存定时器时间的变量重新赋值，以此证明有定时器被注册
                        timer = ctx.timerService().currentProcessingTime() + 5000;
                        ctx.timerService().registerProcessingTimeTimer(timer);
                    }
                } else {
                    //水位没有上升则删除原来的定时器
                    System.out.println("删除定时器");
                    ctx.timerService().deleteProcessingTimeTimer(timer);
                    timer = Long.MIN_VALUE;
                }
                //将当前的水位保存到变量中以便下次来做对比
                lastVc = value.getVc();
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                //将报警信息输出到侧输出流
                ctx.output(new OutputTag<String>("output"){}, "报警！！！！连续5S水位上升" + ctx.getCurrentKey());
                //重置定时器
                timer = Long.MIN_VALUE;
            }
        });
        process.print("主流");

        process.getSideOutput(new OutputTag<String>("output") {
        }).print("报警信息:");

        env.execute();
    }
}
