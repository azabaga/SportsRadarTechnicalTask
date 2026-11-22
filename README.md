# Offer service

The Java 21 and Spring Boot service loads the offer from two filesystem JSON files, serves the visible offer over HTTP, and periodically removes expired events from memory. [STI-2](https://azabaga.atlassian.net/browse/STI-2)

## Run locally

Run these commands from the project root with a Java 21 JDK. The Maven Wrapper downloads Maven 3.9.12 and the build resolves dependencies from Maven Central on its first run.

```sh
java -version
./mvnw test
./mvnw clean package
java -jar target/offer-service-0.1.0.jar
```

For a development run, use `./mvnw spring-boot:run`. On Windows, use `mvnw.cmd` in place of `./mvnw`.

| Spring property | Default | Meaning |
| --- | --- | --- |
| `offer.markets-file` | `fixtures/appendix-1-markets.json` | Path to the JSON array of market definitions |
| `offer.events-file` | `fixtures/appendix-1-events.json` | Path to the JSON array of events |
| `offer.cleanup-interval-ms` | `30000` | Positive delay in milliseconds between the end of one cleanup run and the start of the next |
| `server.port` | `8080` | HTTP port |

Relative paths resolve from the process working directory. To use different data files or run from a different directory, pass absolute paths.

```sh
java -jar target/offer-service-0.1.0.jar \
  --offer.markets-file=/path/to/markets.json \
  --offer.events-file=/path/to/events.json \
  --offer.cleanup-interval-ms=10000
```

Both files must be readable and valid before the service starts. Inputs use numeric status codes `0` (inactive) and `1` (active). Required fields and IDs are validated, with duplicate IDs rejected within their owning collection. An explicit empty array is valid. Zone-less Appendix 1 timestamps are interpreted as UTC. Timestamps with explicit offsets are converted to UTC instants. The JSON field is `odds`, matching the appendices, though the PDF entity table calls it `odd`.

## HTTP API

`GET /offer` returns the visible events for the current time. Optional `from` and `to` parameters accept UTC calendar dates in `YYYY-MM-DD` format. The dates are inclusive from midnight at the start of `from` through the last nanosecond of `to`. Either bound can be used on its own. Invalid, blank, or repeated dates and reversed ranges return HTTP 400 with a problem detail. An offer with no matches returns HTTP 200 with three empty arrays.

Every request uses one current instant. The PDF excludes an event once the current time is later than `startsAt`, so an event remains eligible at its exact start instant. Cleanup uses that same strict comparison and removes both active and inactive events once their start is in the past.

An event, its event market, the referenced market, its event market outcome, and the referenced outcome within that market must all be active. A missing reference hides the affected item and stays in the source state until it can be resolved or the event is cleaned up. Event markets without visible outcomes and events without visible markets are omitted.

```sh
curl -fsS 'http://localhost:8080/offer?from=2032-05-01&to=2032-05-01'
```

With the supplied Appendix 1 data, this request returns the following response through `2032-05-01T20:00:00Z` inclusive. After that instant it returns three empty arrays.

```json
{
  "events": [
    {
      "id": "1",
      "name": "Real Barcelona",
      "startsAt": "2032-05-01T20:00:00Z",
      "markets": [
        {
          "id": "1-1",
          "marketId": "3way",
          "outcomes": [{"id": "1-1-1", "outcomeId": "3way-1", "odds": 1.5513}]
        }
      ]
    }
  ],
  "markets": [{"id": "3way", "name": "3 way", "outcomeIds": ["3way-1"]}],
  "marketOutcomes": [{"marketId": "3way", "id": "3way-1", "name": "1"}]
}
```

An event market's `marketId` refers to `markets[].id`. Resolve an `outcomeId` using that `marketId` and `marketOutcomes[].id`. Each definition appears once, only if a visible event instance uses it after date filtering. Events are sorted by start time then ID, event markets and their outcomes by instance ID, market definitions by ID, and outcome definitions by market ID then outcome ID. Statuses are omitted from the response because every returned item is active.

## State and update behavior

- `OfferApplication` uses `FilesystemOfferLoader` to load and validate both files before creating the store. The JSON decoder handles the assignment's numeric statuses and timestamp format. The domain records contain immutable lists and reference market definitions by ID.
- `InMemoryOfferStore` serializes updates, cleanup and the capture of both collections with one lock. Each request takes a shallow immutable snapshot and projects it outside the lock. Separate market and event updates are individually atomic.
- `OfferController` validates the query. `OfferService` filters events, resolves only active references, and collects the definitions actually used. `OfferCleanup` applies the same clock rule to remove records from the store.
- `updateMarket` replaces the full definition. `updateEvent` uses the incoming event metadata and merges market instances by ID. Each supplied instance replaces its full outcome list, and absent instances remain.

STI-3 will connect those update methods to explicitly configured Spring Kafka clients and define ordering, offsets, and error handling. State currently starts from the configured files on every launch. Broker-based restart and replay are tracked in STI-4.
