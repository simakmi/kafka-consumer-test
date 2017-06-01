package ru.smi.test.kafka.deserializer;

import com.alibaba.fastjson.JSON;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class FastJsonDeserializer implements Deserializer {

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            return JSON.parseObject(new String(data, "UTF8"), Map.class);
        } catch (UnsupportedEncodingException exc) {
            throw new SerializationException(exc);
        }
    }

    @Override
    public void close() {

    }

}
