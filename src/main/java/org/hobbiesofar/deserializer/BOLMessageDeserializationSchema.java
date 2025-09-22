package org.hobbiesofar.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.hobbiesofar.dto.BOL;
import org.hobbiesofar.dto.Transaction;

import java.io.IOException;

public class BOLMessageDeserializationSchema implements DeserializationSchema<BOL> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public BOL deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, BOL.class);
    }

    @Override
    public void deserialize(byte[] message, Collector<BOL> out) throws IOException {
        DeserializationSchema.super.deserialize(message, out);
    }

    @Override
    public boolean isEndOfStream(BOL bol) {
        return false;
    }

    @Override
    public TypeInformation<BOL> getProducedType() {
        return TypeInformation.of(BOL.class);
    }
}
