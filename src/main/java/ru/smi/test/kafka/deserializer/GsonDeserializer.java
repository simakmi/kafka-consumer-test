package ru.smi.test.kafka.deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class GsonDeserializer implements Deserializer {

    private static final Gson gson = new GsonBuilder().create();

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            return gson.fromJson(new String(data, "UTF8"), Map.class);
        } catch (UnsupportedEncodingException exc) {
            throw new SerializationException(exc);
        }
    }

    @Override
    public void close() {

    }

}
