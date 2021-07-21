package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

import static org.apache.flink.table.api.Expressions.$;

public class Flink05_TableAPI_Connect_Kafka {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.连接Kafka系统，获取Kafka中的数据
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());

        tableEnv.connect(
                new Kafka()
                .version("universal")
                .topic("sensor")
                .property("group.id", "bigdata0225")
                .startFromLatest()
                .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")

        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //4.TableAPI将临时表转为Table对象，为了调用相关算子
        Table table = tableEnv.from("sensor");

        Table result = table
                .groupBy($("id"))
                .aggregate($("id").count().as("count"))
                .select($("id"), $("count"));


        //通过调用execute这个方法返回一个TableResult类型可以直接打印
        TableResult execute = result.execute();
        execute.print();


    }
}
