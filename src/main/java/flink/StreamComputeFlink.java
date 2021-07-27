package flink;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;

import com.alibaba.fastjson.JSONObject;

import flink.dto.Behavior;

public class StreamComputeFlink {
	public static void main(String[] args) throws Exception {
		// Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// get message from kafka
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");
		DataStreamSource<String> stream = env
				.addSource(new FlinkKafkaConsumer<>("user_behavior", new SimpleStringSchema(), properties));
		//System.out.println("1111111111");
		SingleOutputStreamOperator<Behavior> behaviorStream = stream.map((MapFunction<String, Behavior>) s -> {
			//String[] split = s.split(",");
			//Long ts = Long.parseLong(split[4]) * 1000;
			//System.out.println("2222222222");

			Behavior behavior = JSONObject.parseObject(s,Behavior.class);
			
			return behavior;
		}).assignTimestampsAndWatermarks(
				WatermarkStrategy.<Behavior>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
						(SerializableTimestampAssigner<Behavior>) (behavior, l) -> {
						
							SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
							Date date;
							Long timestamp=null;
							try {
								date = format.parse(behavior.getTs());
								timestamp=date.getTime()*1000;
							} catch (ParseException e) {
								e.printStackTrace();
							}
							return timestamp;
						}));
		//System.out.println("3333333333");
		//behaviorStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))).process(new BehaviorCountWinFunc()).addSink(esSinkTenSec.build());
		
		
		// ES 配置 
		List<HttpHost> httpHosts = new ArrayList<HttpHost>();
		httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

		// 构建一个 ElasticsearchSink 的 bulider
		ElasticsearchSink.Builder<Object[]> esSinkTenSec = new ElasticsearchSink.Builder<Object[]>(
		    httpHosts, new BehaviorCountSinkFunc("statis-user-behavior-10s"));
		
		esSinkTenSec.setBulkFlushMaxActions(1);

		behaviorStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
		        .process(new BehaviorCountWinFunc())
		        .addSink(esSinkTenSec.build());
		System.out.println("4444444444");
		env.execute();
		System.out.println("555555555");
	}

}
