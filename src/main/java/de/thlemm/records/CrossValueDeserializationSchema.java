package de.thlemm.records;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class CrossValueDeserializationSchema implements DeserializationSchema<CrossValue> {
    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CrossValue deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, CrossValue.class);
    }

    @Override
    public boolean isEndOfStream(CrossValue nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CrossValue> getProducedType() {
        return TypeInformation.of(CrossValue.class);
    }
}