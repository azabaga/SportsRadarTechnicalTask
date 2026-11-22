package com.example.offer.application;

import com.example.offer.domain.Event;
import com.example.offer.domain.EventMarket;
import com.example.offer.domain.EventMarketOutcome;
import com.example.offer.domain.Market;
import com.example.offer.domain.MarketOutcome;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.example.offer.domain.Status.ACTIVE;
import static java.time.ZoneOffset.UTC;

@Service
public final class OfferService {
    private final InMemoryOfferStore store;
    private final Clock clock;

    public OfferService(InMemoryOfferStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    public Offer getOffer(LocalDate from, LocalDate to) {
        var snapshot = store.snapshot();
        Instant now = clock.instant();
        Instant earliest = from == null ? Instant.MIN : from.atStartOfDay().toInstant(UTC);
        Instant latest = to == null ? Instant.MAX : to.atTime(LocalTime.MAX).toInstant(UTC);

        Map<String, Map<String, MarketOutcome>> activeOutcomes = new HashMap<>();
        Map<String, Map<String, MarketOutcome>> usedOutcomes = new TreeMap<>();
        List<Offer.VisibleEvent> events = new ArrayList<>();
        for (Event event : snapshot.events()) {
            if (event.status() != ACTIVE || event.hasStarted(now)
                    || event.startsAt().isBefore(earliest) || event.startsAt().isAfter(latest)) {
                continue;
            }

            List<Offer.VisibleMarket> markets = new ArrayList<>();
            for (EventMarket eventMarket : event.markets()) {
                Market market = snapshot.markets().get(eventMarket.marketId());
                if (eventMarket.status() != ACTIVE || market == null || market.status() != ACTIVE) {
                    continue;
                }
                var visible = projectMarket(eventMarket, activeOutcomes.computeIfAbsent(market.id(),
                        ignored -> indexActiveOutcomes(market)), usedOutcomes);
                if (!visible.outcomes().isEmpty()) {
                    markets.add(visible);
                }
            }
            if (!markets.isEmpty()) {
                markets.sort(Comparator.comparing(Offer.VisibleMarket::id));
                events.add(new Offer.VisibleEvent(event.id(), event.name(), event.startsAt(), List.copyOf(markets)));
            }
        }

        events.sort(Comparator.comparing(Offer.VisibleEvent::startsAt).thenComparing(Offer.VisibleEvent::id));
        List<Offer.MarketDefinition> marketDefinitions = new ArrayList<>();
        List<Offer.OutcomeDefinition> outcomeDefinitions = new ArrayList<>();
        usedOutcomes.forEach((marketId, outcomes) -> {
            Market market = snapshot.markets().get(marketId);
            marketDefinitions.add(new Offer.MarketDefinition(marketId, market.name(), List.copyOf(outcomes.keySet())));
            outcomes.values().forEach(outcome ->
                    outcomeDefinitions.add(new Offer.OutcomeDefinition(marketId, outcome.id(), outcome.name())));
        });
        return new Offer(List.copyOf(events), List.copyOf(marketDefinitions), List.copyOf(outcomeDefinitions));
    }

    private static Map<String, MarketOutcome> indexActiveOutcomes(Market market) {
        return market.outcomes().stream()
                .filter(outcome -> outcome.status() == ACTIVE)
                .collect(Collectors.toMap(MarketOutcome::id, Function.identity()));
    }

    private static Offer.VisibleMarket projectMarket(
            EventMarket eventMarket,
            Map<String, MarketOutcome> activeOutcomes,
            Map<String, Map<String, MarketOutcome>> usedOutcomes
    ) {
        List<Offer.VisibleOutcome> outcomes = new ArrayList<>();
        for (EventMarketOutcome eventOutcome : eventMarket.outcomes()) {
            MarketOutcome outcome = activeOutcomes.get(eventOutcome.outcomeId());
            if (eventOutcome.status() == ACTIVE && outcome != null) {
                outcomes.add(new Offer.VisibleOutcome(eventOutcome.id(), outcome.id(), eventOutcome.odds()));
                usedOutcomes.computeIfAbsent(eventMarket.marketId(), ignored -> new TreeMap<>())
                        .put(outcome.id(), outcome);
            }
        }
        outcomes.sort(Comparator.comparing(Offer.VisibleOutcome::id));
        return new Offer.VisibleMarket(eventMarket.id(), eventMarket.marketId(), List.copyOf(outcomes));
    }
}
