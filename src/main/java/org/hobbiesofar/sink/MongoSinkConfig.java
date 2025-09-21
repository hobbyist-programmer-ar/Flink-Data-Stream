package org.hobbiesofar.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.bson.BsonDocument;
import org.hobbiesofar.dto.Transaction;

public class MongoSinkConfig {
    public static MongoSink<Transaction> generateMongSink() {
        return MongoSink.<Transaction>builder()
                .setUri("DBSTRING")
                .setDatabase("MyTransactions")
                .setCollection("FinancialTransactions")
                .setBatchSize(1000)
                .setBatchIntervalMs(1000)
                .setMaxRetries(3)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
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
    }
}
