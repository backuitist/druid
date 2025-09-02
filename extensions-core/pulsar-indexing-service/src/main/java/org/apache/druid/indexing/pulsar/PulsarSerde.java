package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.pulsar.client.api.MessageId;

import java.io.IOException;

public class PulsarSerde {
    public static class MessageIdSer extends JsonSerializer<MessageId> {
        @Override
        public void serialize(MessageId value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeBinary(value.toByteArray());
        }
    }

    public static class MessageIdDeser extends JsonDeserializer<MessageId> {
        @Override
        public MessageId deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            return MessageId.fromByteArray(p.getBinaryValue());
        }
    }
}
