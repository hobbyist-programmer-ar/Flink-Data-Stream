/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hobbiesofar;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.client.model.InsertOneModel;
import lombok.Value;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.BsonDocument;
import org.hobbiesofar.deserializer.JSONValueDeserializationSchema;
import org.hobbiesofar.dto.Transaction;

public class DataStreamJob {
	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String topic = "financial-txn";
		KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics(topic)
				.setGroupId("financial-transaction-cg")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationSchema())
				.build();
		DataStream<Transaction> transactionDataStream = env
				.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		transactionDataStream.print();
		MongoSink<Transaction> sink = MongoSink.<Transaction>builder()
				.setUri("DBSTRING")
				.setDatabase("MyTransactions")
				.setCollection("FinancialTransactions")
				.setBatchSize(1000)
				.setBatchIntervalMs(1000)
				.setMaxRetries(3)
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.setSerializationSchema(
						(txn, context) ->{
							ObjectMapper objectMapper = new ObjectMapper();
                            String json = null;
                            try {
                                json = objectMapper.writeValueAsString(txn);
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                            return new InsertOneModel<>(BsonDocument.parse(json));
						})
				.build();
		transactionDataStream.sinkTo(sink);
		env.execute("Flink Java API Skeleton");
	}
}
