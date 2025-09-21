package org.hobbiesofar.source;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.hobbiesofar.deserializer.JSONValueDeserializationSchema;
import org.hobbiesofar.dto.Transaction;

public class KafkaSourceConfig {
    public static KafkaSource<Transaction> getKafkaSourceConfig(){
        String topic = "financial-txn";
        return KafkaSource.<Transaction>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("financial-transaction-cg")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();
    }
}
