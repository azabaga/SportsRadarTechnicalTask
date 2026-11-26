package io.github.azabaga.offer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

final class KafkaReplay {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReplay.class);

    // Run before the HTTP server, scheduler and live consumer group start.
    static void restore(Consumer<String, String> consumer, Collection<TopicPartition> partitions,
                        KafkaOfferUpdates updates) {
        consumer.assign(partitions);
        var ends = consumer.endOffsets(partitions);
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        var pending = new HashSet<>(partitions);
        consumer.beginningOffsets(partitions).forEach((partition, start) -> {
            if (start != 0) {
                throw new IllegalStateException("Incomplete replay history for " + partition + " starts at " + start);
            }
            consumer.seek(partition, start);
            offsets.put(partition, new OffsetAndMetadata(ends.get(partition)));
            if (ends.get(partition).equals(start)) {
                pending.remove(partition);
                consumer.pause(List.of(partition));
            }
        });
        LOG.info("Restoring offer through exclusive partition offsets {}", ends);
        while (!pending.isEmpty()) {
            for (var record : consumer.poll(Duration.ofSeconds(1))) {
                var partition = new TopicPartition(record.topic(), record.partition());
                if (record.offset() < ends.get(partition)) {
                    if (KafkaUpdateConfiguration.MARKET_TOPIC.equals(record.topic())) {
                        updates.updateMarket(record);
                    } else {
                        updates.updateEvent(record);
                    }
                }
            }
            pending.removeIf(partition -> {
                if (consumer.position(partition) < ends.get(partition)) {
                    return false;
                }
                consumer.pause(List.of(partition));
                return true;
            });
        }
        consumer.commitSync(offsets);
        LOG.info("Offer restored. Live consumers will resume at {}", ends);
    }

    private KafkaReplay() {}
}
