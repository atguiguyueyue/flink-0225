package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink08_UDF_AggFun {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        //将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //TODO 4.不注册函数直接使用 TableAPI
//        table
//                .groupBy($("id"))
//                .select($("id"), call(MyAvgFun.class, $("vc")).as("vcAvg")).execute().print();
        //TODO 4.先注册再使用  TableAPI
        tableEnv.createTemporarySystemFunction("vcAvg", MyAvgFun.class);

//        table
//        .groupBy($("id"))
//        .select($("id"),call("vcAvg", $("vc")).as("vcAvg" )).execute().print();
        ;


        //SQL 写法
        tableEnv.executeSql("select id,vcAvg(vc) from " + table+" group by id").print();


    }
    //自定义一个累加器
    public static class MyAccumulat{
        public Integer vcSum;
        public Integer count;

    }

    //自定义表函数，求平均数
    public static class MyAvgFun extends AggregateFunction<Double, MyAccumulat>{

        /**
         * 初始化累加器
         * @return
         */
        @Override
        public MyAccumulat createAccumulator() {
            MyAccumulat accumulat = new MyAccumulat();
            accumulat.count = 0;
            accumulat.vcSum = 0;
            return accumulat;
        }

        /**
         * 累加操作
         * @param acc
         * @param value
         */
        public void accumulate(MyAccumulat acc,Integer value){
            acc.vcSum += value;
            acc.count += 1;
        }

        /**
         * 获取最终结果
         * @param accumulator
         * @return
         */
        @Override
        public Double getValue(MyAccumulat accumulator) {
            return accumulator.vcSum*1D/accumulator.count;
        }
    }


}
