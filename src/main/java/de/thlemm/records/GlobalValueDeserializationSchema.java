package de.thlemm.records;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * A Kafka {@link DeserializationSchema} to deserialize {@link GlobalValue}s from JSON.
 *
 */
public class GlobalValueDeserializationSchema  implements DeserializationSchema<GlobalValue> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public GlobalValue deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, GlobalValue.class);
    }

    @Override
    public boolean isEndOfStream(GlobalValue nextElement) {
        return false;
    }

    @Override
    public TypeInformation<GlobalValue> getProducedType() {
        return TypeInformation.of(GlobalValue.class);
    }
}
