package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;

public class Flink03_TableAPI_Connect_File {
    public static void main(String[] args) throws Exception {
        //1.创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.连接文件系统，获取文件中的数据
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        tableEnv.connect(new FileSystem().path("input/sensor_sql.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //4.TableAPI将临时表转为Table对象，为了调用相关算子
        Table table = tableEnv.from("sensor");

        Table result = table
                .select($("id"), $("ts"), $("vc"));

        //SQl写法
//        Table result = tableEnv.sqlQuery("select * from sensor");

        //通过调用execute这个方法返回一个TableResult类型可以直接打印
        TableResult execute = result.execute();
        execute.print();

        //5.将表转为流
//        tableEnv.toAppendStream(result, Row.class).print();

//        env.execute();

    }
}
