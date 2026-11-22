package com.example.offer.application;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;

@Component
public final class OfferCleanup {
    private final InMemoryOfferStore store;
    private final Clock clock;

    public OfferCleanup(InMemoryOfferStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${offer.cleanup-interval-ms}")
    public void removeStartedEvents() {
        store.removeStartedEvents(clock.instant());
    }
}
