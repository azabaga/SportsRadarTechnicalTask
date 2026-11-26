package io.github.azabaga.offer.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

/** Reject corrupt identifiers and JSON strings before a record can update the offer. */
public final class Utf8Deserializer implements Deserializer<String> {
    @Override
    public String deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(data)).toString();
        } catch (CharacterCodingException malformed) {
            throw new SerializationException("Invalid UTF-8 on topic " + topic, malformed);
        }
    }
}
