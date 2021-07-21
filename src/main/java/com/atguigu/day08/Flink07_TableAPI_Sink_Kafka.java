package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class Flink07_TableAPI_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));


        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.1将流转为表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        Table result = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));


        //TODO 3.连接Kafka将数据写入Kafka
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .sinkPartitionerRoundRobin()
                        .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")

        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");


        //将查询出来的数据写入Kafka临时表中
        result.executeInsert("sensor");

    }
}
