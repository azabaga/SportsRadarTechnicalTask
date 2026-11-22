package com.example.offer.application;

import com.example.offer.domain.Event;
import com.example.offer.domain.EventMarket;
import com.example.offer.domain.Market;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class InMemoryOfferStore {
    private final Map<String, Market> markets = new HashMap<>();
    private final Map<String, Event> events = new HashMap<>();

    public InMemoryOfferStore(List<Market> initialMarkets, List<Event> initialEvents) {
        initialMarkets.forEach(market -> markets.put(market.id(), market));
        initialEvents.forEach(event -> events.put(event.id(), event));
    }

    public synchronized void updateMarket(Market market) {
        markets.put(market.id(), market);
    }

    public synchronized void updateEvent(Event event) {
        events.merge(event.id(), event, InMemoryOfferStore::mergeEventMarkets);
    }

    private static Event mergeEventMarkets(Event current, Event update) {
        Map<String, EventMarket> merged = new LinkedHashMap<>();
        current.markets().forEach(market -> merged.put(market.id(), market));
        update.markets().forEach(market -> merged.put(market.id(), market));
        return new Event(update.id(), update.name(), update.startsAt(), update.status(),
                List.copyOf(merged.values()));
    }

    public synchronized void removeStartedEvents(Instant now) {
        events.values().removeIf(event -> event.hasStarted(now));
    }

    synchronized Snapshot snapshot() {
        // Both collections are copied under the same lock. Their domain values are immutable.
        return new Snapshot(Map.copyOf(markets), List.copyOf(events.values()));
    }

    record Snapshot(Map<String, Market> markets, List<Event> events) {
    }
}
