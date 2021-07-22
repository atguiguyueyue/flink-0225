package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink07_UDF_TableFun {
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
//                .joinLateral(call(MyTableFun.class, $("id")))
//                .select($("id"),$("word")).execute().print();
        //TODO 4.先注册再使用  TableAPI
        tableEnv.createTemporarySystemFunction("myTableFun", MyTableFun.class);

//        table
//                .joinLateral(call("myTableFun", $("id")))
//                .select($("id"),$("word")).execute().print();

        //SQL 写法
//        tableEnv.executeSql("select id,word from " + table + " join Lateral table (myTableFun(id)) on true").print();
        tableEnv.executeSql("select id,word from " + table + ", Lateral table (myTableFun(id))").print();

    }
    //自定义表函数，按照下划线切分id
    @FunctionHint(output = @DataTypeHint("Row<word STRING>"))
    public static class MyTableFun extends TableFunction<Row>{
        public void eval(String value){
            String[] split = value.split("_");
            for (String s : split) {
                collect(Row.of(s));
            }
        }
    }

}
