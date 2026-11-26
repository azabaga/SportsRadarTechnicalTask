package io.github.azabaga.offer;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("offer")
record OfferProperties(
        @NotBlank String marketsFile,
        @NotBlank String eventsFile,
        @Positive long cleanupIntervalMs
) {
}
