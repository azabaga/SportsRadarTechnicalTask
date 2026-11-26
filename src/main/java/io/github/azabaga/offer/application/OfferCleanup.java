package io.github.azabaga.offer.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;

@Component
public final class OfferCleanup {
    private static final Logger LOG = LoggerFactory.getLogger(OfferCleanup.class);
    private final InMemoryOfferStore store;
    private final Clock clock;

    public OfferCleanup(InMemoryOfferStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${offer.cleanup-interval-ms}")
    public void removeStartedEvents() {
        int removed = store.removeStartedEvents(clock.instant());
        if (removed > 0) {
            LOG.info("Removed {} started event(s)", removed);
        }
    }
}
