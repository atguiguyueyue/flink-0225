package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink05_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件读取数据
//        DataStreamSource<String> streamSource = env.readTextFile("input/waterSensor.txt");
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将从文件读古来的数据线转为waterSensor，在转为Json
        SingleOutputStreamOperator<WaterSensor> DStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO JDBC Sink
        DStream.addSink(JdbcSink.sink(
                "insert into sensor values(?,?,?)",
                (JdbcStatementBuilder<WaterSensor>) (preparedStatement, waterSensor) -> {
                    preparedStatement.setString(1, waterSensor.getId());
                    preparedStatement.setLong(2, waterSensor.getTs());
                    preparedStatement.setInt(3, waterSensor.getVc());
                    System.out.println("111111");
                },
                //TODO 读取无界流时，指定条数，达到这个数据条数，则把数据写入Mysql
//                JdbcExecutionOptions.builder().withBatchSize(1).build(),
                //TODO 读取无界流时，指定间隔时间，达到这个时间，则把数据写入Mysql
                JdbcExecutionOptions.builder().withBatchIntervalMs(10000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                        .withUsername("root")
                        .withDriverName(Driver.class.getName())
                        .withPassword("000000")
                        .build()));

        env.execute();
    }


}
