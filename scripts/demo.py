"""Run from the project root against a dedicated, initially empty Compose stack."""
import json
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen

BASE = "http://localhost:" + os.environ.get("OFFER_PORT", "8080")


def offer(query=""):
    with urlopen(BASE + "/offer" + query, timeout=4) as response:
        return json.load(response)


def publish(topic, payload):
    subprocess.run(["docker", "compose", "exec", "-T", "kafka",
                    "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server",
                    "kafka:29092", "--topic", topic, "--reader-property", "parse.key=true",
                    "--reader-property", "key.separator=|"],
                   input=payload["id"] + "|" + json.dumps(payload) + "\n", text=True, check=True)


def wait_for(label, predicate, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            print(label, flush=True)
            return
        time.sleep(0.25)
    raise RuntimeError("Timed out: " + label)


def event_markets(event_id):
    return next((e["markets"] for e in offer()["events"] if e["id"] == event_id), [])


initial = offer("?from=2032-05-01&to=2032-05-01")
assert [event["name"] for event in initial["events"]] == ["Real Barcelona"]
print("Initial offer and date filter:", json.dumps(initial), flush=True)
market = json.loads(Path("fixtures/appendix-2-market.json").read_text())
event = json.loads(Path("fixtures/appendix-2-event.json").read_text())
publish("market", market)
publish("event", event)
wait_for("Appendix 2 event consumed", lambda: bool(event_markets("3")))
extra = {"id": "3-2", "marketId": "total", "status": 1, "outcomes": [
    {"id": "3-2-1", "outcomeId": "total-0.5", "status": 1, "odds": 2.5}]}
publish("event", dict(event, markets=[extra]))
wait_for("Delta preserved 3-1 and added 3-2",
         lambda: [m["id"] for m in event_markets("3")] == ["3-1", "3-2"])
with_total = offer()
without_total = dict(market, status=0)
publish("market", without_total)
wait_for("Inactive total market hidden from event 3",
         lambda: [m["id"] for m in event_markets("3")] == ["3-1"])
assert all(m["id"] != "total" for m in offer()["markets"])
publish("market", market)
wait_for("Market reactivation restored the original offer", lambda: offer() == with_total)
publish("market", dict(market, outcomes=[dict(o, status=0) if o["id"] == "total-0.5" else o
                                        for o in market["outcomes"]]))
wait_for("Inactive outcome hid the last selection in 3-2",
         lambda: [m["id"] for m in event_markets("3")] == ["3-1"])
assert all(o["marketId"] != "total" for o in offer()["marketOutcomes"])
publish("market", market)
wait_for("Outcome reactivation restored the original offer", lambda: offer() == with_total)
assert offer("?from=2032-05-01&to=2032-05-01") == initial
before_restart = offer()
subprocess.run(["docker", "compose", "restart", "app"], check=True)
subprocess.run(["docker", "compose", "up", "-d", "--wait", "app"], check=True)
assert offer() == before_restart
print("Offer identical after application restart", flush=True)
expires = datetime.now(timezone.utc) + timedelta(seconds=12)
publish("event", dict(event, id="demo-expiry", startsAt=expires.isoformat(), markets=[extra]))
wait_for("Short-lived event visible", lambda: bool(event_markets("demo-expiry")))
wait_for("Short-lived event expired", lambda: not event_markets("demo-expiry"))
since = expires.isoformat(timespec="seconds")
wait_for("Scheduled removal confirmed in app logs", lambda: "Removed " in subprocess.check_output(
    ["docker", "compose", "logs", "--since", since, "app"], text=True))
