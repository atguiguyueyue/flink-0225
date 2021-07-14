package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将从端口读古来的数据线转为waterSensor，在转为Json
        SingleOutputStreamOperator<WaterSensor> jsonDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });


        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();
        jsonDStream.addSink(new RedisSink<>(flinkJedisPoolConfig,new MyRedisSink()));

        env.execute();
    }
    public static class MyRedisSink implements RedisMapper<WaterSensor>{

        /**
         * 插入数据的命令,当使用两个参数的构造方法时，一般是Hash类型的，第二个参数指定的是Redis的大Key
         * @return
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
//            return new RedisCommandDescription(RedisCommand.HSET,System.currentTimeMillis()+"");
            return new RedisCommandDescription(RedisCommand.SET);
        }

        /**
         * 指定RedisKey(当时Hash时，这个key为小key即filed)，默认情况下是redisKey
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(WaterSensor data) {
            return data.getId();
        }

        /**
         * 指定插入的数据
         * @param data
         * @return
         */
        @Override
        public String getValueFromData(WaterSensor data) {
            return JSONObject.toJSONString(data);
        }
    }
}
