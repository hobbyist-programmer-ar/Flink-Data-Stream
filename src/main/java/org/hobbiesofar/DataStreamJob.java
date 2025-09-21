package org.hobbiesofar;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.hobbiesofar.dto.Order;
import org.hobbiesofar.dto.Transaction;
import org.hobbiesofar.mapper.OrderMapper;

import static org.hobbiesofar.sink.MongoSinkConfig.generateMongSink;
import static org.hobbiesofar.source.KafkaSourceConfig.getKafkaSourceConfig;

public class DataStreamJob {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		KafkaSource<Transaction> source = getKafkaSourceConfig();
		DataStream<Transaction> transactionDataStream = env
				.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		transactionDataStream.print();
		DataStream<Order> orderDataStream = transactionDataStream.map(new OrderMapper());
		orderDataStream.print();
		MongoSink<Transaction> sink = generateMongSink();
		transactionDataStream.sinkTo(sink);
		env.execute("Flink Java API Skeleton");
	}
}
