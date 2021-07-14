package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将从端口读古来的数据线转为waterSensor，在转为Json
        SingleOutputStreamOperator<WaterSensor> DStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //创建泛型为HttpHost的一个list集合用来存放ES的地址
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));
        httpHosts.add(new HttpHost("hadoop103", 9200));
        httpHosts.add(new HttpHost("hadoop104", 9200));

        ElasticsearchSink.Builder<WaterSensor> sensorBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {

                //创建Index请求，可以指定要写入的索引，类型，id
                IndexRequest indexRequest = new IndexRequest("sensor-0225", "_doc", element.getId());
                //将数据转为json并声明为json，写入ES
                indexRequest.source(JSON.toJSONString(element), XContentType.JSON);

                indexer.add(indexRequest);
            }
        });

        //因为读的是无界流，所以需要设置刷写时数据的条数，设置为1代表，来一条往ES中写一条
        sensorBuilder.setBulkFlushMaxActions(1);

        ElasticsearchSink<WaterSensor> elasticsearchSink = sensorBuilder.build();

        DStream.addSink(elasticsearchSink);

        env.execute();
    }

}
