package com.example.offer.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record EventMarket(
        @NotBlank String id,
        @NotBlank String marketId,
        @NotNull Status status,
        List<@Valid EventMarketOutcome> outcomes
) {
    public EventMarket {
        outcomes = EntityIds.uniqueList(outcomes, EventMarketOutcome::id, "event market outcome");
    }
}
