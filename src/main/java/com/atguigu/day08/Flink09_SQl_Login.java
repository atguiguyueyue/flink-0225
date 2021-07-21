package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink09_SQl_Login {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        //2.创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    /*    //3.将流转为表 -> （未注册的表）
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //TODO 4.注册未注册的表(方式一)
        tableEnv.createTemporaryView("sensor", table);*/

        //TODO 4.在流转为表的时候，直接转为注册表(方式二)
        tableEnv.createTemporaryView("sensor", waterSensorStream);

        //5.使用SQL进行查询
        tableEnv.executeSql("select * from sensor where id = 'sensor_1'").print();

    }
}
