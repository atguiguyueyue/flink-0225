package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink02_TableAPI_Agg_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));


        //TODO 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 将流转为动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        /**
         *select
         * id,
         * sum(vc)
         * from table
         * group by id
         */
        //TODO 查询表
//        Table result = table
//                .groupBy($("id"))
//                .aggregate($("vc").sum().as("vcSum"))
//                .select($("id"), $("vcSum"));
        Table result = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum());

        //TODO 将动态表转为流
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(result, Row.class);

        rowDataStream.print();

        env.execute();

    }
}
