package io.github.azabaga.offer.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.List;

public record Event(
        @NotBlank String id,
        @NotBlank String name,
        @NotNull Instant startsAt,
        @NotNull Status status,
        List<@Valid EventMarket> markets
) {
    public Event {
        markets = EntityIds.uniqueList(markets, EventMarket::id, "event market");
    }

    public boolean hasStarted(Instant now) {
        return startsAt.isBefore(now);
    }
}
