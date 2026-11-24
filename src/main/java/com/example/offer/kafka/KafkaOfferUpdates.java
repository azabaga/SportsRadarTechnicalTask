package com.example.offer.kafka;

import com.example.offer.application.InMemoryOfferStore;
import com.example.offer.domain.Event;
import com.example.offer.domain.Market;
import com.example.offer.json.OfferJson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

import static com.example.offer.kafka.KafkaUpdateConfiguration.EVENT_TOPIC;
import static com.example.offer.kafka.KafkaUpdateConfiguration.MARKET_TOPIC;

class KafkaOfferUpdates {
    private final InMemoryOfferStore store;
    private final OfferJson json;

    KafkaOfferUpdates(InMemoryOfferStore store, OfferJson json) {
        this.store = store;
        this.json = json;
    }

    @KafkaListener(id = "market-updates", idIsGroup = false, topics = MARKET_TOPIC,
            containerFactory = "offerKafkaListenerContainerFactory")
    public void updateMarket(ConsumerRecord<String, String> record) {
        try {
            store.updateMarket(json.read(record.value(), Market.class));
        } catch (RuntimeException failure) {
            throw failedUpdate(record, failure);
        }
    }

    @KafkaListener(id = "event-updates", idIsGroup = false, topics = EVENT_TOPIC, concurrency = "2",
            containerFactory = "offerKafkaListenerContainerFactory")
    public void updateEvent(ConsumerRecord<String, String> record) {
        try {
            Event event = json.read(record.value(), Event.class);
            if (!event.id().equals(record.key())) {
                throw new IllegalArgumentException("The event Kafka key must match the payload ID");
            }
            store.updateEvent(event);
        } catch (RuntimeException failure) {
            throw failedUpdate(record, failure);
        }
    }

    private static IllegalStateException failedUpdate(ConsumerRecord<String, String> record, RuntimeException cause) {
        return new IllegalStateException("Offer update failed at topic=%s partition=%d offset=%d"
                .formatted(record.topic(), record.partition(), record.offset()), cause);
    }
}
