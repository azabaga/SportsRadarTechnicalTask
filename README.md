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

## Kafka updates

Start a local plaintext Apache Kafka broker in one terminal. The application creates `market` with one partition and `event` with two, using replication factor one for new topics. It checks those counts on startup, and accepts an existing `event` topic with more than two partitions.

```sh
docker run --rm --name offer-kafka -p 127.0.0.1:9092:9092 apache/kafka:4.2.2
```

Once the broker is ready, run the application with the `kafka` Spring profile from the project root.

```sh
./mvnw spring-boot:run -Dspring-boot.run.profiles=kafka
```

The profile defaults to `offer.kafka.bootstrap-servers=localhost:9092` and `offer.kafka.group-id=offer-service`. Override them with Spring command-line properties when using an executable JAR. Without the profile, the service runs from the JSON files alone.

Each Kafka value is one UTF-8 JSON object in the Appendix 2 format. The `market` topic expects the full definition, including the full outcome list. The `event` topic expects event metadata and a list of event market instances. An event record's string key must match its JSON `id`. Updates merge using each event market instance's `id` (for example `1-1`), while `marketId` continues to reference the shared definition (for example `3way`).

To see an event update on the API, publish the following record. The text before the first pipe is the Kafka key.

```sh
docker exec -i offer-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic event \
  --property parse.key=true --property 'key.separator=|' <<'EOF'
1|{"id":"1","name":"Real Barcelona","startsAt":"2032-05-01T20:00:00","status":1,"markets":[{"id":"1-1","marketId":"3way","status":1,"outcomes":[{"id":"1-1-1","outcomeId":"3way-1","status":1,"odds":1.9}]}]}
EOF
curl -fsS 'http://localhost:8080/offer'
```

Once consumed, the response shows odds of `1.9` for the updated outcome.

### Ordering, offsets and failures

- **Ordering.** Each partition is consumed in offset order. The single market partition orders all market updates. Producers should use the event ID as the key with Kafka's standard partitioner and leave the partition unspecified. Updates for the same event then share a partition while the partition count is fixed. Different event partitions and the two topics progress independently. The HTTP API may observe a market or event update arriving before its counterpart, and applies the existing reference visibility rules.
- **Offsets.** Auto-commit is disabled. The consumer commits offsets synchronously, one record at a time after the listener has applied the update to the in-memory store. A successful update can be delivered again if its offset was not committed. A group with no usable offset starts at the earliest retained record.
- **Failures.** Invalid JSON, invalid domain data, null values, missing or mismatched event keys, and exceptions while applying a record stop the affected topic's listener container. The failure log identifies the topic, partition and offset. The failed record stays uncommitted, so subsequent records in its partition wait for operator intervention. The other topic listener and HTTP service can continue with the state already applied. Inspect the record and fix the cause before resuming consumption. A permanently invalid record requires an explicit operator decision to skip its offset.
- **State lifetime.** Run one application instance for one complete offer. Each instance has its own memory, and Kafka consumer groups distribute partitions between instances. A process restart loads the fixture files again and the existing Kafka group resumes from committed offsets. Restoring the complete offer across restarts is tracked in STI-4.

### Real broker integration test

Run on a fresh, dedicated broker because the test writes to `market` and `event`, including a deliberate invalid record. Start a new container with the command above, then run:

```sh
./mvnw -Pkafka-it clean verify
```

The test checks the partition counts, publishes updates, waits for the HTTP response to reflect event and market changes, and confirms an invalid event key stops consumption without advancing past the rejected record. Use `-Dkafka.it.bootstrap-servers=localhost:19092` to target a different broker address.
