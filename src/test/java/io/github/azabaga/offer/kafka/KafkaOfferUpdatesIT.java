package io.github.azabaga.offer.kafka;

import io.github.azabaga.offer.OfferApplication;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class KafkaOfferUpdatesIT {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String BOOTSTRAP = System.getProperty("kafka.it.bootstrap-servers");

    @BeforeAll
    static void requireDedicatedBroker() throws Exception {
        assertNotNull(BOOTSTRAP, "Set kafka.it.bootstrap-servers to a fresh, dedicated broker");
        try (var admin = Admin.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
            var existing = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertTrue(Collections.disjoint(existing, List.of("market", "event")),
                    "This test writes invalid records. Use a fresh broker without market or event topics.");
        }
    }

    @Test
    void refusesToResizeAnExistingEventTopic() throws Exception {
        try (var admin = Admin.create(Map.of("bootstrap.servers", BOOTSTRAP))) {
            var event = "undersized-" + UUID.randomUUID();
            var market = "market-" + UUID.randomUUID();
            admin.createTopics(List.of(new NewTopic(event, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            var failure = assertThrows(IllegalStateException.class,
                    () -> KafkaUpdateConfiguration.provisionTopics(admin, market, event));
            assertTrue(failure.getMessage().contains("at least two event partitions"));
            var topics = admin.describeTopics(List.of(event)).allTopicNames().get(10, TimeUnit.SECONDS);
            assertEquals(1, topics.get(event).partitions().size());
        }
    }

    @Test
    void consumesUpdatesAndKeepsTheRejectedOffsetUncommitted() throws Exception {
        String bootstrap = BOOTSTRAP;
        String group = "offer-it-" + UUID.randomUUID();
        var connection = Map.<String, Object>of("bootstrap.servers", bootstrap);
        var producerProperties = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000
        );

        try (var admin = Admin.create(connection);
             var producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer());
             var context = start(bootstrap, group);
             var http = HttpClient.newHttpClient()) {
            var topics = admin.describeTopics(List.of("market", "event")).allTopicNames().get(10, TimeUnit.SECONDS);
            assertEquals(1, topics.get("market").partitions().size());
            assertTrue(topics.get("event").partitions().size() >= 2);

            var request = HttpRequest.newBuilder(URI.create(
                            "http://127.0.0.1:" + context.getWebServer().getPort() + "/offer"))
                    .timeout(Duration.ofSeconds(5)).build();
            assertEquals("Real Barcelona", findById(getOffer(http, request).path("events"), "1").path("name").asString());

            var seeded = producer.send(new ProducerRecord<>("event", "1", """
                    {"id":"1","name":"Real Barcelona","startsAt":"2032-05-01T20:00:00","status":1,"markets":[
                      {"id":"1-1","marketId":"3way","status":1,"outcomes":[
                        {"id":"1-1-1","outcomeId":"3way-1","status":1,"odds":1.7},
                        {"id":"1-1-2","outcomeId":"3way-2","status":1,"odds":2.1}]},
                      {"id":"1-2","marketId":"3way","status":1,"outcomes":[
                        {"id":"1-2-1","outcomeId":"3way-1","status":1,"odds":1.8}]}]}
                    """)).get(10, TimeUnit.SECONDS);
            await("both event markets to appear", () -> {
                var markets = findById(getOffer(http, request).path("events"), "1").path("markets");
                return findById(markets, "1-1").path("outcomes").size() == 2
                        && findById(markets, "1-2").path("outcomes").size() == 1;
            });

            var delta = producer.send(new ProducerRecord<>("event", "1", """
                    {"id":"1","name":"Real Barcelona updated","startsAt":"2032-05-01T20:00:00","status":1,"markets":[
                      {"id":"1-1","marketId":"3way","status":1,"outcomes":[
                        {"id":"1-1-2","outcomeId":"3way-2","status":1,"odds":2.6}]}]}
                    """)).get(10, TimeUnit.SECONDS);
            assertEquals(seeded.partition(), delta.partition());
            await("the delta to replace one outcome list and preserve the omitted market", () -> {
                var event = findById(getOffer(http, request).path("events"), "1");
                var updated = findById(event.path("markets"), "1-1").path("outcomes");
                var preserved = findById(event.path("markets"), "1-2").path("outcomes");
                return event.path("name").asString().equals("Real Barcelona updated")
                        && updated.size() == 1 && findById(updated, "1-1-2").path("odds").asDouble() == 2.6
                        && preserved.size() == 1 && findById(preserved, "1-2-1").path("odds").asDouble() == 1.8;
            });

            var replacement = producer.send(new ProducerRecord<>("market", "3way", """
                    {"id":"3way","name":"Match winner","status":1,"outcomes":[
                      {"id":"3way-1","name":"Home","status":1}]}
                    """)).get(10, TimeUnit.SECONDS);
            await("the replacement market to hide the removed outcome and expose the new definition", () -> {
                var offer = getOffer(http, request);
                var event = findById(offer.path("events"), "1");
                var definitions = offer.path("marketOutcomes");
                return findById(offer.path("markets"), "3way").path("name").asString().equals("Match winner")
                        && findById(definitions, "3way-1").path("name").asString().equals("Home")
                        && findById(definitions, "3way-2").isMissingNode()
                        && findById(event.path("markets"), "1-1").isMissingNode()
                        && !findById(event.path("markets"), "1-2").isMissingNode();
            });

            var eventPartition = new TopicPartition("event", delta.partition());
            var marketPartition = new TopicPartition("market", replacement.partition());
            await("both update offsets to be committed before restart", () -> {
                var offsets = admin.listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                var eventOffset = offsets.get(eventPartition);
                var marketOffset = offsets.get(marketPartition);
                return eventOffset != null && eventOffset.offset() == delta.offset() + 1
                        && marketOffset != null && marketOffset.offset() == replacement.offset() + 1;
            });

            var beforeRestart = getOffer(http, request);
            context.close();
            try (var restarted = start(bootstrap, group)) {
                var restoredRequest = HttpRequest.newBuilder(URI.create("http://127.0.0.1:"
                        + restarted.getWebServer().getPort() + "/offer")).timeout(Duration.ofSeconds(5)).build();
                assertEquals(beforeRestart, getOffer(http, restoredRequest));
                var rejected = producer.send(new ProducerRecord<>("event", "1", """
                        {"id":"different","name":"Wrong key","startsAt":"2032-05-01T20:00:00","status":1,"markets":[]}
                        """)).get(10, TimeUnit.SECONDS);
                var later = producer.send(new ProducerRecord<>("event", "1", """
                        {"id":"1","name":"This record must wait","startsAt":"2032-05-01T20:00:00","status":1,"markets":[]}
                        """)).get(10, TimeUnit.SECONDS);
                assertEquals(delta.partition(), rejected.partition());
                assertEquals(rejected.partition(), later.partition());
                var registry = restarted.getBean(KafkaListenerEndpointRegistry.class);
                await("the event listener to stop", () -> !registry.getListenerContainer("event-updates").isRunning());

                var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                assertEquals(rejected.offset(), offsets.get(new TopicPartition("event", rejected.partition())).offset());
                assertEquals("Real Barcelona updated", findById(getOffer(http, restoredRequest).path("events"), "1").path("name").asString());
                var ready = HttpRequest.newBuilder(restoredRequest.uri().resolve("/health/ready"))
                        .timeout(Duration.ofSeconds(5)).build();
                assertEquals(503, http.send(ready, HttpResponse.BodyHandlers.ofString()).statusCode());

                var invalidJson = producer.send(new ProducerRecord<>("market", "3way", "{"))
                        .get(10, TimeUnit.SECONDS);
                await("the market listener to stop on malformed JSON",
                        () -> !registry.getListenerContainer("market-updates").isRunning());
                var finalOffsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS);
                assertEquals(invalidJson.offset(), finalOffsets.get(marketPartition).offset());
                assertEquals(beforeRestart, getOffer(http, restoredRequest));
            }
            var failure = assertThrows(org.springframework.beans.BeansException.class, () -> {
                try (var unexpected = start(bootstrap, group)) {
                    fail("Replay should reject corrupt history before startup completes");
                }
            });
            assertTrue(failure.getMessage().contains("Offer update failed at topic="), failure::getMessage);
        }
    }

    private static ServletWebServerApplicationContext start(String bootstrap, String group) {
        return (ServletWebServerApplicationContext) new SpringApplicationBuilder(OfferApplication.class)
                     .profiles("kafka")
                     .initializers(application -> ((GenericApplicationContext) application).registerBean(
                             "fixedClock", Clock.class,
                             () -> Clock.fixed(Instant.parse("2030-06-01T20:00:00Z"), ZoneOffset.UTC),
                             definition -> definition.setPrimary(true)))
                     .run("--server.address=127.0.0.1", "--server.port=0", "--spring.main.banner-mode=off",
                             "--logging.level.root=WARN", "--offer.kafka.bootstrap-servers=" + bootstrap,
                             "--offer.kafka.group-id=" + group);
    }

    private static JsonNode getOffer(HttpClient http, HttpRequest request) throws Exception {
        var response = http.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        return JSON.readTree(response.body());
    }

    private static JsonNode findById(JsonNode array, String id) {
        for (JsonNode item : array) {
            if (id.equals(item.path("id").asString())) {
                return item;
            }
        }
        return array.path(-1);
    }

    private static void await(String description, Condition condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.satisfied()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for " + description);
    }

    @FunctionalInterface
    private interface Condition {
        boolean satisfied() throws Exception;
    }
}
