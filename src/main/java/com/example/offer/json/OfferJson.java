package com.example.offer.json;

import com.example.offer.domain.Status;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.cfg.CoercionAction;
import tools.jackson.databind.cfg.CoercionInputShape;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.type.LogicalType;

import java.io.InputStream;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalQueries;
import java.util.List;

@Component
public final class OfferJson {
    private final JsonMapper mapper = JsonMapper.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                    DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
                    DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES,
                    DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                    DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .withCoercionConfig(LogicalType.Textual, config -> config
                    .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
                    .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                    .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail))
            .addModule(new SimpleModule()
                    .addDeserializer(Status.class, new StatusDeserializer())
                    .addDeserializer(Instant.class, new StartTimeDeserializer()))
            .build();
    private final Validator validator;

    public OfferJson(Validator validator) {
        this.validator = validator;
    }

    public <T> T read(String payload, Class<T> type) {
        if (payload == null) {
            throw new IllegalArgumentException("Expected a JSON object");
        }
        T entry = mapper.readValue(payload, type);
        validate(entry, type.getSimpleName());
        return entry;
    }

    public <T> List<T> readList(InputStream input, Class<T> type) {
        var listType = mapper.getTypeFactory().constructCollectionType(List.class, type);
        List<T> entries = mapper.readValue(input, listType);
        if (entries == null) {
            throw new IllegalArgumentException("Expected a JSON array");
        }
        for (int i = 0; i < entries.size(); i++) {
            validate(entries.get(i), "Entry " + i);
        }
        return List.copyOf(entries);
    }

    private <T> void validate(T entry, String description) {
        if (entry == null) {
            throw new IllegalArgumentException(description + " must be an object");
        }
        var violations = validator.validate(entry);
        if (!violations.isEmpty()) {
            var errors = violations.stream()
                    .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                    .sorted()
                    .toList();
            throw new IllegalArgumentException(description + ": " + String.join(", ", errors));
        }
    }

    private static final class StatusDeserializer extends StdDeserializer<Status> {
        private StatusDeserializer() {
            super(Status.class);
        }

        @Override
        public Status deserialize(JsonParser parser, DeserializationContext context) {
            int code = parser.currentToken() == JsonToken.VALUE_NUMBER_INT ? parser.getIntValue() : -1;
            return switch (code) {
                case 0 -> Status.INACTIVE;
                case 1 -> Status.ACTIVE;
                default -> context.reportInputMismatch(Status.class, "status must be the integer 0 or 1");
            };
        }
    }

    private static final class StartTimeDeserializer extends StdDeserializer<Instant> {
        private StartTimeDeserializer() {
            super(Instant.class);
        }

        @Override
        public Instant deserialize(JsonParser parser, DeserializationContext context) {
            if (parser.currentToken() != JsonToken.VALUE_STRING) {
                return context.reportInputMismatch(Instant.class, "startsAt must be an ISO-8601 timestamp");
            }
            try {
                var time = DateTimeFormatter.ISO_DATE_TIME.parse(parser.getString());
                ZoneOffset offset = time.query(TemporalQueries.offset());
                return LocalDateTime.from(time).toInstant(offset == null ? ZoneOffset.UTC : offset);
            } catch (DateTimeException invalidTime) {
                return context.reportInputMismatch(Instant.class, "startsAt must be an ISO-8601 timestamp");
            }
        }
    }
}
