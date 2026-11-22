package com.example.offer.application;

import com.example.offer.OfferApplication;
import com.example.offer.domain.Event;
import com.example.offer.domain.EventMarket;
import com.example.offer.domain.EventMarketOutcome;
import com.example.offer.domain.Market;
import com.example.offer.domain.MarketOutcome;
import com.example.offer.domain.Status;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.example.offer.domain.Status.ACTIVE;
import static com.example.offer.domain.Status.INACTIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OfferServiceTest {
    private static final Instant NOW = Instant.parse("2030-06-01T20:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void appliesAllVisibilityRulesAndRetainsUnresolvedReferencesForLaterUpdates() {
        var market = market("m", ACTIVE,
                definition("used", ACTIVE), definition("inactive-definition", INACTIVE),
                definition("inactive-instance", ACTIVE));
        var otherMarket = market("other", ACTIVE, definition("only-in-other-market", ACTIVE));
        var inactiveMarket = market("inactive-market", INACTIVE, definition("used", ACTIVE));
        var valid = eventMarket("a-valid", "m", ACTIVE,
                outcome("1", "used", ACTIVE),
                outcome("2", "inactive-instance", INACTIVE),
                outcome("3", "inactive-definition", ACTIVE),
                outcome("4", "only-in-other-market", ACTIVE));
        var empty = eventMarket("b-empty", "m", ACTIVE, outcome("1", "missing", ACTIVE));
        var missing = eventMarket("c-missing", "missing-market", ACTIVE, outcome("1", "used", ACTIVE));
        var inactiveDefinition = eventMarket("d-inactive-definition", "inactive-market", ACTIVE,
                outcome("1", "used", ACTIVE));
        var inactiveInstance = eventMarket("e-inactive-instance", "m", INACTIVE, outcome("1", "used", ACTIVE));

        var store = new InMemoryOfferStore(List.of(market, otherMarket, inactiveMarket), List.of(
                event("visible", NOW, ACTIVE, valid, empty, missing, inactiveDefinition, inactiveInstance),
                event("inactive", NOW, INACTIVE, valid),
                event("started", NOW.minusNanos(1), ACTIVE, valid),
                event("empty", NOW, ACTIVE, empty),
                event("unresolved", NOW, ACTIVE, missing)));
        var service = new OfferService(store, CLOCK);

        var offer = service.getOffer(null, null);
        assertEquals(List.of("visible"), eventIds(offer));
        var visibleMarkets = offer.events().getFirst().markets();
        assertEquals(List.of(new Offer.VisibleMarket("a-valid", "m",
                List.of(new Offer.VisibleOutcome("1", "used", 1.5)))), visibleMarkets);
        assertEquals(List.of(new Offer.MarketDefinition("m", "m", List.of("used"))), offer.markets());
        assertEquals(List.of(new Offer.OutcomeDefinition("m", "used", "used")), offer.marketOutcomes());
        assertEquals(5, store.snapshot().events().stream()
                .filter(event -> event.id().equals("visible")).findFirst().orElseThrow().markets().size());

        store.updateMarket(market("missing-market", ACTIVE, definition("used", ACTIVE)));
        assertEquals(List.of("unresolved", "visible"), eventIds(service.getOffer(null, null)));

        store.updateMarket(market("m", ACTIVE, definition("used", ACTIVE), definition("missing", ACTIVE)));
        assertEquals(List.of("empty", "unresolved", "visible"), eventIds(service.getOffer(null, null)));
    }

    @Test
    void deduplicatesDefinitionsAndOrdersThemAfterDateFiltering() {
        var shared = market("a", ACTIVE,
                definition("only-first-day", ACTIVE), definition("shared", ACTIVE));
        var separate = market("b", ACTIVE, definition("shared", ACTIVE));
        var dayOne = eventMarket("x", "a", ACTIVE,
                outcome("z", "shared", ACTIVE), outcome("a", "only-first-day", ACTIVE));
        var dayTwo = eventMarket("x", "a", ACTIVE, outcome("b", "shared", ACTIVE));
        var secondMarket = eventMarket("a", "b", ACTIVE, outcome("c", "shared", ACTIVE));
        var store = new InMemoryOfferStore(List.of(separate, shared), List.of(
                event("b", NOW, ACTIVE, dayOne),
                event("a", NOW, ACTIVE, dayOne),
                event("c", NOW.plusSeconds(86_400), ACTIVE, dayTwo, secondMarket)));
        var service = new OfferService(store, CLOCK);

        var all = service.getOffer(null, null);
        assertEquals(List.of("a", "b", "c"), eventIds(all));
        assertEquals(List.of("a", "z"),
                all.events().getFirst().markets().getFirst().outcomes().stream().map(Offer.VisibleOutcome::id).toList());
        assertEquals(List.of("a", "x"),
                all.events().getLast().markets().stream().map(Offer.VisibleMarket::id).toList());
        assertEquals(List.of(
                new Offer.OutcomeDefinition("a", "only-first-day", "only-first-day"),
                new Offer.OutcomeDefinition("a", "shared", "shared"),
                new Offer.OutcomeDefinition("b", "shared", "shared")), all.marketOutcomes());

        var filtered = service.getOffer(LocalDate.parse("2030-06-02"), null);
        assertEquals(List.of("c"), eventIds(filtered));
        assertEquals(List.of(new Offer.MarketDefinition("a", "a", List.of("shared")),
                new Offer.MarketDefinition("b", "b", List.of("shared"))), filtered.markets());
        assertEquals(List.of(new Offer.OutcomeDefinition("a", "shared", "shared"),
                new Offer.OutcomeDefinition("b", "shared", "shared")), filtered.marketOutcomes());
        assertEquals(List.of("a"), service.getOffer(null, LocalDate.parse("2030-06-01")).markets().stream()
                .map(Offer.MarketDefinition::id).toList());
    }

    @Test
    void includesBothEndsOfTheUtcCalendarDayAndTheExactStartInstant() {
        var visible = eventMarket("em", "m", ACTIVE, outcome("eo", "used", ACTIVE));
        var store = new InMemoryOfferStore(List.of(market("m", ACTIVE, definition("used", ACTIVE))), List.of(
                event("just-started", NOW.minusNanos(1), ACTIVE, visible),
                event("now", NOW, ACTIVE, visible),
                event("midnight", Instant.parse("2030-06-02T00:00:00Z"), ACTIVE, visible),
                event("end-of-day", Instant.parse("2030-06-02T23:59:59.999999999Z"), ACTIVE, visible),
                event("next-midnight", Instant.parse("2030-06-03T00:00:00Z"), ACTIVE, visible)));
        var service = new OfferService(store, CLOCK);
        var june2 = LocalDate.parse("2030-06-02");

        assertEquals(List.of("now", "midnight", "end-of-day", "next-midnight"), eventIds(service.getOffer(null, null)));
        assertEquals(List.of("midnight", "end-of-day"), eventIds(service.getOffer(june2, june2)));
        assertEquals(List.of("now", "midnight", "end-of-day"), eventIds(service.getOffer(null, june2)));
        assertEquals(List.of("midnight", "end-of-day", "next-midnight"), eventIds(service.getOffer(june2, null)));

        Clock nextTick = Clock.offset(CLOCK, Duration.ofNanos(1));
        assertEquals(List.of("midnight", "end-of-day", "next-midnight"),
                eventIds(new OfferService(store, nextTick).getOffer(null, null)));
        assertEquals(5, store.snapshot().events().size());
    }

    @Test
    void cleanupRemovesOnlyEventsWhoseStartIsStrictlyBeforeNow() {
        var store = new InMemoryOfferStore(List.of(), List.of(
                event("started-active", NOW.minusNanos(1), ACTIVE),
                event("started-inactive", NOW.minusSeconds(1), INACTIVE),
                event("now", NOW, ACTIVE),
                event("future", NOW.plusNanos(1), INACTIVE)));

        new OfferCleanup(store, CLOCK).removeStartedEvents();

        assertEquals(List.of("future", "now"), store.snapshot().events().stream().map(Event::id).sorted().toList());
    }

    @Test
    void scheduledCleanupRemovesAnExpiredEventAddedAfterStartup() {
        try (var context = new SpringApplicationBuilder(OfferApplication.class)
                .web(WebApplicationType.NONE)
                .run("--offer.cleanup-interval-ms=20", "--spring.main.banner-mode=off", "--logging.level.root=OFF")) {
            var store = context.getBean(InMemoryOfferStore.class);
            store.updateEvent(event("expired-update", Instant.EPOCH, ACTIVE));
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                while (store.snapshot().events().stream().anyMatch(event -> event.id().equals("expired-update"))) {
                    Thread.sleep(10);
                }
            }, "The scheduled cleanup should remove the expired event");
        }
    }

    @Test
    void updatesReplaceMarketDefinitionsAndMergeEventMarketsWithCompleteOutcomeLists() {
        var a = eventMarket("a", "m", ACTIVE,
                outcome("a1", "first", ACTIVE), outcome("a2", "removed", ACTIVE));
        var b = eventMarket("b", "untouched", ACTIVE, outcome("b1", "keep", ACTIVE));
        var initial = event("event", NOW, ACTIVE, a, b);
        var store = new InMemoryOfferStore(List.of(
                market("m", ACTIVE, definition("first", ACTIVE), definition("removed", ACTIVE)),
                market("untouched", ACTIVE, definition("keep", ACTIVE))), List.of(initial));
        var oldSnapshot = store.snapshot();
        var replacement = new Market("m", "new name", ACTIVE, List.of(new MarketOutcome("first", "new label", ACTIVE)));
        var updatedA = eventMarket("a", "m", ACTIVE, new EventMarketOutcome("a1", "first", ACTIVE, 2.0));
        var added = eventMarket("c", "m", ACTIVE, outcome("c1", "first", ACTIVE));

        store.updateMarket(replacement);
        store.updateEvent(new Event("event", "new event name", NOW.plusSeconds(10), ACTIVE, List.of(updatedA, added)));

        var snapshot = store.snapshot();
        assertEquals(replacement, snapshot.markets().get("m"));
        assertEquals(new Event("event", "new event name", NOW.plusSeconds(10), ACTIVE, List.of(updatedA, b, added)),
                snapshot.events().getFirst());
        assertEquals(initial, oldSnapshot.events().getFirst());
        assertEquals(2, oldSnapshot.markets().get("m").outcomes().size());
        var offer = new OfferService(store, CLOCK).getOffer(null, null);
        assertEquals("new name", offer.markets().getFirst().name());
        assertEquals("new label", offer.marketOutcomes().getFirst().name());
        assertEquals(List.of(new Offer.VisibleOutcome("a1", "first", 2.0)),
                offer.events().getFirst().markets().getFirst().outcomes());

        store.updateEvent(event("new-event", NOW, ACTIVE, updatedA));
        assertTrue(eventIds(new OfferService(store, CLOCK).getOffer(null, null)).contains("new-event"));
    }

    @Test
    void concurrentDeltasForOneEventPreserveBothMarkets() throws Exception {
        var store = new InMemoryOfferStore(List.of(), List.of(event("event", NOW, ACTIVE)));
        var first = eventMarket("a", "m", ACTIVE);
        var second = eventMarket("b", "m", ACTIVE);
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(2)) {
            var left = executor.submit(() -> {
                ready.countDown();
                start.await();
                store.updateEvent(event("event", NOW, ACTIVE, first));
                return null;
            });
            var right = executor.submit(() -> {
                ready.countDown();
                start.await();
                store.updateEvent(event("event", NOW, ACTIVE, second));
                return null;
            });
            boolean bothReady = ready.await(5, TimeUnit.SECONDS);
            start.countDown();
            assertTrue(bothReady);
            left.get(5, TimeUnit.SECONDS);
            right.get(5, TimeUnit.SECONDS);
        }
        assertEquals(List.of("a", "b"), store.snapshot().events().getFirst().markets().stream()
                .map(EventMarket::id).sorted().toList());
    }

    private static List<String> eventIds(Offer offer) {
        return offer.events().stream().map(Offer.VisibleEvent::id).toList();
    }

    private static Market market(String id, Status status, MarketOutcome... outcomes) {
        return new Market(id, id, status, List.of(outcomes));
    }

    private static MarketOutcome definition(String id, Status status) {
        return new MarketOutcome(id, id, status);
    }

    private static Event event(String id, Instant startsAt, Status status, EventMarket... markets) {
        return new Event(id, id, startsAt, status, List.of(markets));
    }

    private static EventMarket eventMarket(String id, String marketId, Status status, EventMarketOutcome... outcomes) {
        return new EventMarket(id, marketId, status, List.of(outcomes));
    }

    private static EventMarketOutcome outcome(String id, String outcomeId, Status status) {
        return new EventMarketOutcome(id, outcomeId, status, 1.5);
    }
}
