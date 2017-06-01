package ru.smi.test.kafka.deserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JacksonDeserializer implements Deserializer {

    private static final ObjectReader reader = new ObjectMapper().readerFor(new TypeReference<Map<String, Object>>() {
    });

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            return reader.readValue(data);
        } catch (IOException exc) {
            throw new SerializationException(exc);
        }
    }

    @Override
    public void close() {

    }

}
