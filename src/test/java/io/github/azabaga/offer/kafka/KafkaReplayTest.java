package io.github.azabaga.offer.kafka;

import io.github.azabaga.offer.application.InMemoryOfferStore;
import io.github.azabaga.offer.application.OfferService;
import io.github.azabaga.offer.domain.Market;
import io.github.azabaga.offer.domain.MarketOutcome;
import io.github.azabaga.offer.json.OfferJson;
import jakarta.validation.Validation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.github.azabaga.offer.domain.Status.ACTIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class KafkaReplayTest {
    @Test
    void commitsTheCapturedEndEvenWhenTheFinalPollReadsANewerRecord() throws Exception {
        var partition = new TopicPartition("event", 0);
        var event = Files.readString(Path.of("fixtures/appendix-2-event.json"));
        var market = new Market("3way", "Winner", ACTIVE, List.of(new MarketOutcome("3way-1", "Home", ACTIVE)));
        var store = new InMemoryOfferStore(List.of(market), List.of());
        var service = new OfferService(store, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        try (var consumer = new MockConsumer<String, String>("earliest");
             var validation = Validation.buildDefaultValidatorFactory()) {
            consumer.updateBeginningOffsets(Map.of(partition, 0L));
            consumer.updateEndOffsets(Map.of(partition, 1L));
            consumer.schedulePollTask(() -> {
                consumer.updateEndOffsets(Map.of(partition, 2L));
                consumer.addRecord(new ConsumerRecord<>("event", 0, 0, "3", event));
                consumer.addRecord(new ConsumerRecord<>("event", 0, 1, "3", event.replace("Dev NonDev", "Later")));
            });
            consumer.schedulePollTask(() -> fail("Replay polled beyond its captured end"));

            KafkaReplay.restore(consumer, Set.of(partition),
                    new KafkaOfferUpdates(store, new OfferJson(validation.getValidator())));

            assertEquals("Dev NonDev", service.getOffer(null, null).events().getFirst().name());
            assertEquals(2L, consumer.position(partition));
            assertEquals(1L, consumer.committed(Set.of(partition)).get(partition).offset());
        }
    }
}
