package io.github.azabaga.offer.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Utf8DeserializerTest {
    @Test
    void preservesUnicodeAndRejectsCorruptBytesInsideAJsonString() {
        try (var decoder = new Utf8Deserializer()) {
            var json = "{\"name\":\"Željezničar\"}";
            var bytes = json.getBytes(StandardCharsets.UTF_8);
            assertEquals(json, decoder.deserialize("event", bytes));
            bytes[bytes.length - 3] = (byte) 0xff;
            assertThrows(SerializationException.class, () -> decoder.deserialize("event", bytes));
        }
    }
}
