package com.example.offer.http;

import com.example.offer.OfferApplication;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OfferHttpTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static ServletWebServerApplicationContext context;
    private static HttpClient client;

    @BeforeAll
    static void startServer() {
        context = (ServletWebServerApplicationContext) new SpringApplicationBuilder(OfferApplication.class)
                .initializers(application -> ((GenericApplicationContext) application).registerBean(
                        "fixedClock", Clock.class,
                        () -> Clock.fixed(Instant.parse("2030-06-01T20:00:00Z"), ZoneOffset.UTC),
                        definition -> definition.setPrimary(true)))
                .run("--server.address=127.0.0.1", "--server.port=0",
                        "--spring.main.banner-mode=off", "--logging.level.root=WARN");
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterAll
    static void stopServer() {
        if (client != null) {
            client.close();
        }
        if (context != null) {
            context.close();
        }
    }

    @Test
    void returnsTheVisibleAppendixOfferWithSeparateDefinitions() throws Exception {
        var response = get("?from=2032-05-01&to=2032-05-01");
        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElseThrow().startsWith("application/json"));
        assertEquals(JSON.readTree("""
                {
                  "events": [{
                    "id": "1",
                    "name": "Real Barcelona",
                    "startsAt": "2032-05-01T20:00:00Z",
                    "markets": [{
                      "id": "1-1",
                      "marketId": "3way",
                      "outcomes": [{"id": "1-1-1", "outcomeId": "3way-1", "odds": 1.5513}]
                    }]
                  }],
                  "markets": [{"id": "3way", "name": "3 way", "outcomeIds": ["3way-1"]}],
                  "marketOutcomes": [{"marketId": "3way", "id": "3way-1", "name": "1"}]
                }
                """), JSON.readTree(response.body()));
        var unfiltered = get("");
        assertEquals(200, unfiltered.statusCode());
        assertEquals(JSON.readTree(response.body()), JSON.readTree(unfiltered.body()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"?to=2032-04-30", "?from=2032-05-02"})
    void returnsEmptyArraysWhenEitherDateBoundExcludesTheOnlyUpcomingEvent(String query) throws Exception {
        var response = get(query);
        assertEquals(200, response.statusCode());
        assertEquals(JSON.readTree("""
                {"events":[],"markets":[],"marketOutcomes":[]}
                """), JSON.readTree(response.body()));
    }

    @ParameterizedTest
    @CsvSource(value = {
            "from=tomorrow|must be a valid UTC calendar date",
            "to=2032-02-30|must be a valid UTC calendar date",
            "from=|must be a valid UTC calendar date",
            "from=2032-05-02&to=2032-05-01|must be on or before",
            "from=2032-05-01&from=2032-05-02|may be supplied only once"
    }, delimiter = '|')
    void rejectsInvalidDateFiltersWithAProblemDetail(String query, String expectedDetail) throws Exception {
        var response = get("?" + query);
        assertEquals(400, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type").orElseThrow().startsWith("application/problem+json"));
        assertTrue(JSON.readTree(response.body()).path("detail").asString().contains(expectedDetail), response::body);
    }

    private static HttpResponse<String> get(String query) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + context.getWebServer().getPort() + "/offer" + query))
                .timeout(Duration.ofSeconds(5)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
