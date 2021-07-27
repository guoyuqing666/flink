package kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class GenerateKafkaMessage {
	public static void main(String[] args) throws IOException {
		String kafkaTopic = "user_behavior";
		// 使用本地和默认端口
		String brokers = "localhost:9092";

		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", brokers);
		kafkaProps.put("key.serializer", ByteArraySerializer.class.getCanonicalName());
		kafkaProps.put("value.serializer", ByteArraySerializer.class.getCanonicalName());

		KafkaProducer producer = new KafkaProducer(kafkaProps);
		String file_path = "src/main/resources/user_behavior.log";
		InputStream inputStream= new FileInputStream(new File(file_path));

		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		while (reader.ready()) {
			String line = reader.readLine();
			// 发送数据
			producer.send(new ProducerRecord(kafkaTopic, line.getBytes()));
			System.out.println(line);
		}
		reader.close();
		inputStream.close();

	}

}
