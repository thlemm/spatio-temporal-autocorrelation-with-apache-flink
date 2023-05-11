package de.thlemm.records;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class LocalValueDeserializationSchema implements DeserializationSchema<LocalValue> {
    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public LocalValue deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, LocalValue.class);
    }

    @Override
    public boolean isEndOfStream(LocalValue nextElement) {
        return false;
    }

    @Override
    public TypeInformation<LocalValue> getProducedType() {
        return TypeInformation.of(LocalValue.class);
    }
}
