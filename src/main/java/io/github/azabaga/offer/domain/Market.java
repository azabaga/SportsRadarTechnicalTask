package io.github.azabaga.offer.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record Market(
        @NotBlank String id,
        @NotBlank String name,
        @NotNull Status status,
        List<@Valid MarketOutcome> outcomes
) {
    public Market {
        outcomes = EntityIds.uniqueList(outcomes, MarketOutcome::id, "market outcome");
    }
}
