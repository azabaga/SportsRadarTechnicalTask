package com.example.offer.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record EventMarketOutcome(
        @NotBlank String id,
        @NotBlank String outcomeId,
        @NotNull Status status,
        double odds
) {
    public EventMarketOutcome {
        if (!Double.isFinite(odds)) {
            throw new IllegalArgumentException("odds must be a finite number");
        }
    }
}
