package io.github.azabaga.offer.filesystem;

import io.github.azabaga.offer.OfferApplication;
import io.github.azabaga.offer.domain.Event;
import io.github.azabaga.offer.domain.Market;
import io.github.azabaga.offer.domain.MarketOutcome;
import io.github.azabaga.offer.json.OfferJson;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static io.github.azabaga.offer.domain.Status.ACTIVE;
import static io.github.azabaga.offer.domain.Status.INACTIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilesystemOfferLoaderTest {
    @TempDir
    Path directory;

    @Test
    void loadsBothAppendixFilesIncludingInactiveAndExpiredSourceData() {
        try (var validatorFactory = Validation.buildDefaultValidatorFactory()) {
            var loader = new FilesystemOfferLoader(new OfferJson(validatorFactory.getValidator()));
            var markets = loader.read(Path.of("fixtures/appendix-1-markets.json"), Market.class, Market::id);
            var events = loader.read(Path.of("fixtures/appendix-1-events.json"), Event.class, Event::id);

            assertEquals(List.of("3way", "2way"), markets.stream().map(Market::id).toList());
            assertEquals(INACTIVE, markets.getLast().status());
            assertEquals(new MarketOutcome("3way-X", "X", INACTIVE), markets.getFirst().outcomes().getLast());
            assertEquals(List.of("1", "2"), events.stream().map(Event::id).toList());
            var first = events.getFirst();
            assertEquals(Instant.parse("2032-05-01T20:00:00Z"), first.startsAt());
            assertEquals(Instant.parse("2014-06-02T20:00:00Z"), events.getLast().startsAt());
            var eventMarket = first.markets().getFirst();
            assertEquals("3way", eventMarket.marketId());
            assertEquals("3way-1", eventMarket.outcomes().getFirst().outcomeId());
            assertEquals(ACTIVE, eventMarket.outcomes().getFirst().status());
            assertEquals(1.5513, eventMarket.outcomes().getFirst().odds());
        }
    }

    @Test
    void normalizesOffsetTimestampsToUtc() throws IOException {
        var events = Files.writeString(directory.resolve("offset.json"), """
                [{"id":"e","name":"Event","startsAt":"2032-05-02T01:00:00+05:00","status":1,"markets":[]}]
                """);
        try (var validatorFactory = Validation.buildDefaultValidatorFactory()) {
            var loader = new FilesystemOfferLoader(new OfferJson(validatorFactory.getValidator()));
            assertEquals(Instant.parse("2032-05-01T20:00:00Z"),
                    loader.read(events, Event.class, Event::id).getFirst().startsAt());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "null",
            "[null]",
            "[{\"id\":\"x\",\"name\":\"X\",\"status\":2,\"outcomes\":[]}]",
            "[{\"id\":\"x\",\"name\":\"X\",\"status\":\"1\",\"outcomes\":[]}]",
            "[{\"id\":\"x\",\"name\":\"X\",\"outcomes\":[]}]",
            "[{\"id\":3,\"name\":\"X\",\"status\":1,\"outcomes\":[]}]",
            "[{\"id\":\"x\",\"name\":\"X\",\"status\":1,\"outcomes\":[{\"id\":\" \",\"name\":\"Y\",\"status\":1}]}]",
            "[{\"id\":\"x\",\"name\":\"X\",\"status\":1,\"outcomes\":[{\"id\":\"y\",\"name\":\"Y\",\"status\":1},{\"id\":\"y\",\"name\":\"Z\",\"status\":1}]}]",
            "[{\"id\":\"x\",\"name\":\"X\",\"status\":1,\"outcomes\":[]},{\"id\":\"x\",\"name\":\"X\",\"status\":1,\"outcomes\":[]}]",
            "[] []"
    })
    void rejectsInvalidInitialRecords(String json) throws IOException {
        var invalid = Files.writeString(directory.resolve("invalid-markets.json"), json);
        try (var validatorFactory = Validation.buildDefaultValidatorFactory()) {
            var loader = new FilesystemOfferLoader(new OfferJson(validatorFactory.getValidator()));
            var failure = assertThrows(IllegalStateException.class,
                    () -> loader.read(invalid, Market.class, Market::id));
            assertTrue(failure.getMessage().contains(invalid.toString()), failure::getMessage);
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"[{", "[{\"id\":\"e\",\"name\":\"Event\",\"startsAt\":\"2032-02-30T20:00:00\",\"status\":1,\"markets\":[]}]"})
    void abortsApplicationStartupWhenARequiredFileIsMissingOrInvalid(String json) throws IOException {
        var events = directory.resolve("events.json");
        if (json != null) {
            Files.writeString(events, json);
        }

        var failure = assertThrows(RuntimeException.class, () -> {
            try (var context = new SpringApplicationBuilder(OfferApplication.class)
                    .web(WebApplicationType.NONE)
                    .run("--offer.events-file=" + events, "--spring.main.banner-mode=off", "--logging.level.root=OFF")) {
                // A successful startup is a test failure. Close the context if it starts.
            }
        });
        assertTrue(failure.getMessage().contains("Cannot load initial Event data from " + events), failure::getMessage);
    }
}
