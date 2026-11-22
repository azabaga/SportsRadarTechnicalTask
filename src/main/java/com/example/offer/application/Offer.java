package com.example.offer.application;

import java.time.Instant;
import java.util.List;

public record Offer(
        List<VisibleEvent> events,
        List<MarketDefinition> markets,
        List<OutcomeDefinition> marketOutcomes
) {
    public record VisibleEvent(String id, String name, Instant startsAt, List<VisibleMarket> markets) {
    }

    public record VisibleMarket(String id, String marketId, List<VisibleOutcome> outcomes) {
    }

    public record VisibleOutcome(String id, String outcomeId, double odds) {
    }

    public record MarketDefinition(String id, String name, List<String> outcomeIds) {
    }

    public record OutcomeDefinition(String marketId, String id, String name) {
    }
}
