package com.example.offer.filesystem;

import com.example.offer.domain.Status;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalQueries;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

@Component
public final class FilesystemOfferLoader {
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

    public FilesystemOfferLoader(Validator validator) {
        this.validator = validator;
    }

    public <T> List<T> read(Path path, Class<T> type, Function<T, String> identify) {
        Path source = path.toAbsolutePath().normalize();
        try (var input = Files.newInputStream(source)) {
            var listType = mapper.getTypeFactory().constructCollectionType(List.class, type);
            List<T> entries = mapper.readValue(input, listType);
            if (entries == null) {
                throw new IllegalArgumentException("Expected a JSON array");
            }
            Set<String> ids = new HashSet<>();
            for (int i = 0; i < entries.size(); i++) {
                T entry = entries.get(i);
                if (entry == null) {
                    throw new IllegalArgumentException("Entry " + i + " must be an object");
                }
                var violations = validator.validate(entry);
                if (!violations.isEmpty()) {
                    var errors = violations.stream()
                            .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                            .sorted()
                            .toList();
                    throw new IllegalArgumentException("Entry " + i + ": " + String.join(", ", errors));
                }
                String id = identify.apply(entry);
                if (!ids.add(id)) {
                    throw new IllegalArgumentException("Duplicate " + type.getSimpleName() + " ID: " + id);
                }
            }
            return List.copyOf(entries);
        } catch (IOException | JacksonException | IllegalArgumentException error) {
            throw new IllegalStateException("Cannot load initial " + type.getSimpleName()
                    + " data from " + source + ": " + error.getMessage(), error);
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
