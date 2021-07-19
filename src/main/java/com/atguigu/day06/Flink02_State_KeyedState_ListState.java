package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Flink02_State_KeyedState_ListState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将读过来的数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.因为要使用键控状态，所以要进行keyBy操作
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        //5.针对每个传感器输出最高的3个水位值
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, List<Integer>>() {
            //TODO a.定义状态，用来保存三个最高的水位值
            private ListState<Integer> top3Vc;

            @Override
            public void open(Configuration parameters) throws Exception {
                top3Vc = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<List<Integer>> out) throws Exception {

                //将当前数据保存到状态中
                top3Vc.add(value.getVc());

                //取出状态中的数据
                Iterable<Integer> integers = top3Vc.get();

                //将状态中的数据保存到list集合中
                ArrayList<Integer> top3List = new ArrayList<>();
                for (Integer vc : integers) {
                    top3List.add(vc);
                }

                top3List.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });

                //将前三的数据保存下来
                if (top3List.size() > 3) {
                    top3List.remove(3);
                }

              top3Vc.update(top3List);

                out.collect(top3List);

            }
        }).print();


        env.execute();
    }
}
