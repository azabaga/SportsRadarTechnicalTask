package io.github.azabaga.offer;

import io.github.azabaga.offer.application.InMemoryOfferStore;
import io.github.azabaga.offer.domain.Event;
import io.github.azabaga.offer.domain.Market;
import io.github.azabaga.offer.filesystem.FilesystemOfferLoader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.nio.file.Path;
import java.time.Clock;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(OfferProperties.class)
public class OfferApplication {
    public static void main(String[] args) {
        SpringApplication.run(OfferApplication.class, args);
    }

    @Bean
    Clock offerClock() {
        return Clock.systemUTC();
    }

    @Bean
    InMemoryOfferStore offerStore(FilesystemOfferLoader loader, OfferProperties properties) {
        var markets = loader.read(Path.of(properties.marketsFile()), Market.class, Market::id);
        var events = loader.read(Path.of(properties.eventsFile()), Event.class, Event::id);
        return new InMemoryOfferStore(markets, events);
    }
}
