package com.example.offer.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record MarketOutcome(@NotBlank String id, @NotBlank String name, @NotNull Status status) {
}
