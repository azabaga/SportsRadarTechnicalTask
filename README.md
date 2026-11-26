# Offer service

This Java 21 service loads a filesystem seed, applies ordered Kafka updates to an in-memory offer and serves active upcoming events over HTTP.

Project tasks are tracked in the [STI Jira project](https://azabaga.atlassian.net/jira/software/projects/STI/boards/2?filter=&groupBy=none).

## Start the complete service

From the repository root, with Docker Engine and Docker Compose installed, run:

```sh
docker compose up -d --build --wait
curl -fsS http://localhost:8080/health/ready
curl -fsS http://localhost:8080/offer
```

Open [Redpanda Console](http://localhost:8081) and select **Topics**, then **market** or **event**, then **Messages** to inspect records and event keys. Open [Swagger UI](http://localhost:8080/docs.html) to explore the endpoints and run requests. The [OpenAPI YAML](http://localhost:8080/openapi.yaml) is also available. The first build needs internet access to pull images and Maven dependencies. Compose runs the Java build and unit tests in the builder image.

`docker compose ps` shows service status. Use `docker compose logs -f app` to follow replay boundaries, partition assignment, cleanup counts and rejected records. `GET /health/live` checks the HTTP process. With Kafka enabled, `GET /health/ready` returns 200 after startup replay when both topic listeners are running with assignments, or 503 while waiting or after a listener stops. It does not measure consumer lag. Host ports bind to loopback. Override `OFFER_PORT`, `KAFKA_PORT` and `CONSOLE_PORT` if occupied.

`docker compose down` stops the stack and retains Kafka data. The next `docker compose up -d --wait` rebuilds the offer from the seed and retained records.

## Reviewer demo

On a freshly started stack with empty topics, run the following from the project root with Python 3:

```sh
python3 scripts/demo.py
```

The demo prints the filtered Appendix 1 offer and publishes both Appendix 2 fixtures. It adds a second market to event 3, deactivates and reactivates its definition and outcome, and checks the resulting HTTP responses. It compares the offer across an app restart, then publishes a short-lived event and waits for both HTTP expiry and scheduled removal. The fixed appendix examples require the current clock to be before June 2, 2030 at 20:00 UTC. The demo writes to the selected Compose project. To repeat against a fresh dedicated environment, select an unused Compose project name and unused host ports:

```sh
export COMPOSE_PROJECT_NAME=offer-demo2 OFFER_PORT=18080 KAFKA_PORT=19092 CONSOLE_PORT=18081
docker compose up -d --build --wait
python3 scripts/demo.py
docker compose down
```

## Run Java locally without Docker

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
- An alternative seed provider supplies the two domain lists at the store's construction point. Another stream adapter can call the same two update methods. Neither change requires edits to merge or projection rules.

## Kafka updates

Compose starts an unauthenticated Apache Kafka broker. The service creates missing topics with replication factor one, one `market` partition and two `event` partitions. An existing `market` topic must have exactly one partition and an existing `event` topic must have at least two. Incompatible counts fail startup. Resizing a populated event topic can send later records for the same key to a different partition. To develop with the Java process on your host, start only the broker and UI in Compose:

```sh
docker compose up -d --wait kafka console
./mvnw spring-boot:run -Dspring-boot.run.profiles=kafka
```

The Kafka profile defaults to `offer.kafka.bootstrap-servers=localhost:9092` and `offer.kafka.group-id=offer-service`. Without the profile, the service reads just the JSON seed.

Each Kafka value is one UTF-8 JSON object in the Appendix 2 format. The `market` topic expects the full definition, including the full outcome list. The `event` topic expects event metadata and a list of event market instances. An event record's string key must match its JSON `id`. Updates merge using each event market instance's `id` (for example `1-1`), while `marketId` continues to reference the shared definition (for example `3way`).

To see an event update on the API, publish the following record. The text before the first pipe is the Kafka key.

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:29092 --topic event \
  --reader-property parse.key=true --reader-property 'key.separator=|' <<'EOF'
1|{"id":"1","name":"Real Barcelona","startsAt":"2032-05-01T20:00:00","status":1,"markets":[{"id":"1-1","marketId":"3way","status":1,"outcomes":[{"id":"1-1-1","outcomeId":"3way-1","status":1,"odds":1.9}]}]}
EOF
curl -fsS 'http://localhost:8080/offer'
```

Once consumed, the response shows odds of `1.9` for the updated outcome.

### Ordering, offsets and failures

- **Ordering.** Each partition is consumed in offset order. The single market partition orders all market updates. Producers should use the event ID as the key with Kafka's standard partitioner and leave the partition unspecified. Updates for the same event then share a partition while the partition count is fixed. Different event partitions and the two topics progress independently. The HTTP API may observe a market or event update arriving before its counterpart, and applies the existing reference visibility rules.
- **Offsets.** Auto-commit is disabled. The consumer commits offsets synchronously, one record at a time after the listener has applied the update to the in-memory store. A successful update can be delivered again if its offset was not committed. A group with no usable offset starts at the earliest retained record.
- **Failures.** Invalid JSON, invalid domain data, null values, missing or mismatched event keys, and exceptions while applying a record stop the affected topic's listener container. The failure log identifies the topic, partition and offset. The failed record stays uncommitted, so subsequent records in its partition wait for operator intervention. The other topic listener and HTTP service can continue with the state already applied. Inspect the record and fix the cause before resuming consumption. A permanent schema error requires a compatible reader or a reviewed rebuild of the input history. Appending a corrected record or changing a committed offset alone cannot repair an earlier invalid record during replay.

### Recovery contract

Run one app instance per consumer group. At every startup, JSON supplies the baseline. Before HTTP or scheduled cleanup starts, a manually assigned Kafka consumer captures the exclusive end offset of every partition and replays from zero up to those offsets through the same validation and update methods as live listeners. Only after successful replay does it commit those exact boundaries. Live listeners then consume messages at or beyond the boundaries, including records produced during startup. An invalid historical record fails startup. The replay logs the boundary offsets.

Keep the seed unchanged, the event partition count fixed, and all input history intact. The Compose broker uses a named volume, unlimited time/size retention and Kafka's delete policy. Event deltas require full history, so compaction would invalidate recovery. Startup rejects a truncated partition with a nonzero beginning offset. Replacing a topic or deleting the volume destroys history. Disk usage and restart time grow with history. Use a new event ID when reviving a fixture after cleanup. A live delta for a removed ID starts a fresh event in memory. Replay can recover markets from that ID’s earlier history because the removal itself never entered the log.

### Test with a real broker

The integration suite writes deliberately invalid market and event records. It requires an explicit broker address and checks that the `market` and `event` topics are absent before running. Use a fresh dedicated broker. With Java 21 on the host:

```sh
KAFKA_PORT=19093 docker compose -p offer-it up -d --wait kafka
./mvnw -Pkafka-it -Dkafka.it.bootstrap-servers=localhost:19093 clean verify
KAFKA_PORT=19093 docker compose -p offer-it down --volumes
```

The last command deletes only this dedicated test project's volumes so the next run starts with empty topics. The suite covers filesystem validation, HTTP filtering, cleanup, stream merge and replacement, recovery with an existing consumer group, and failure readiness/offsets.
