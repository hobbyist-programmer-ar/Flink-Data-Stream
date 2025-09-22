package org.hobbiesofar.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hobbiesofar.dto.Transaction;

import java.io.IOException;

public class TransactionMessageDeserializationSchema implements DeserializationSchema<Transaction> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public Transaction deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Transaction.class);
    }

    @Override
    public boolean isEndOfStream(Transaction transaction) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}
