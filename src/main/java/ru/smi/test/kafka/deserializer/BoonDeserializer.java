package ru.smi.test.kafka.deserializer;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.boon.json.JsonFactory;
import org.boon.json.JsonParserAndMapper;
import org.boon.json.JsonParserFactory;
import org.boon.json.ObjectMapper;

public class BoonDeserializer implements Deserializer {

    private static final ObjectMapper mapper = JsonFactory.create();
    private static final JsonParserAndMapper parser = createParser();

    private static JsonParserAndMapper createParser() {
        JsonParserFactory factory = new JsonParserFactory();
//        factory.setCharset(StandardCharsets.UTF_8);
        factory.setCheckDates(false);
//        factory.setChop(false);
//        factory.setUseAnnotations(false);
//        JsonParserAndMapper parser = factory.createUTF8DirectByteParser();
        JsonParserAndMapper parser = factory.createFastParser();
//        JsonParserAndMapper parser = new JsonParserFactory().create();
        return parser;
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return parser.parse(data);
    }

    @Override
    public void close() {

    }

}
