package org.apache.druid.indexing.pulsar.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.druid.indexing.pulsar.PulsarSerde;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

public class PulsarSupervisorTest {

    @Test
    public void serializeAMapOfMessageID() throws JsonProcessingException {
        TreeMap<Integer, Map<Integer, MessageIdImpl>> sequenceOffsets = new TreeMap<>();

        Map<Integer, MessageIdImpl> innerMap1 = new TreeMap<>();
        innerMap1.put(1, new MessageIdImpl(101, 201, 0));
        innerMap1.put(2, new MessageIdImpl(102, 202, 0));

        Map<Integer, MessageIdImpl> innerMap2 = new TreeMap<>();
        innerMap2.put(10, new MessageIdImpl(110, 210, 1));
        innerMap2.put(11, new MessageIdImpl(111, 211, 1));

        sequenceOffsets.put(1, innerMap1);
        sequenceOffsets.put(2, innerMap2);

        SimpleModule customModule = new SimpleModule();
        customModule.addSerializer(MessageId.class, new PulsarSerde.MessageIdSer());
        customModule.addDeserializer(MessageId.class, new PulsarSerde.MessageIdDeser());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(customModule);

        String jsonString = objectMapper.writeValueAsString(sequenceOffsets);

        Assert.assertEquals("{\"1\":{\"1\":\"CGUQyQEYADAA\",\"2\":\"CGYQygEYADAA\"}," +
                "\"2\":{\"10\":\"CG4Q0gEYATAA\",\"11\":\"CG8Q0wEYATAA\"}}", jsonString.trim());
    }
}