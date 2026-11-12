"""Streaming fault-tolerance driver with an in-memory fake cluster (no Kafka, Docker or Spark).

The fake keeps topics as lists, kills/starts "brokers" by changing the metadata it returns,
and runs a fake consumer that commits one micro-batch per checkpoint poll. GH Archive is real
public data; the fixture lines are small hand-written look-alikes.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
from pathlib import Path

import pytest
import yaml

from benchmarks import streaming_bench, streaming_fault
from repopulse.runs_log import COLUMNS, validate_row
from repopulse.streaming import fault

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "benchmarks" / "kafka" / "docker-compose.yml"
FIXTURE = ROOT / "tests" / "fixtures" / "gharchive_small" / "2026-01-05-0.json"
SHA = "abc1234"
RUN_AT = "2026-11-11T00:00:00+00:00"
DATE = "2026-10-25"


# ---------------------------------------------------------------- fakes


class FakeClock:
    def __init__(self, start: float = 1000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class FakeProducer:
    """Acks each message at once (or only on flush); can lose or replay chosen messages."""

    def __init__(self, env: FakeEnv):
        self.env = env
        self.pending = []

    def produce(self, topic, key, value, on_delivery):
        env = self.env
        idx = env.produced_count
        env.produced_count += 1
        part = sum(key) % len(env.topics[topic])
        if idx not in env.lose:
            env.topics[topic][part].append(value)
        if idx in env.replay:  # an internal retry written twice: a new offset, one report
            env.topics[topic][part].append(value)
        if env.ack_on_produce:
            on_delivery(None, part)
        else:
            self.pending.append((on_delivery, part))

    def flush(self, timeout):
        for cb, part in self.pending:
            cb(None, part)
        self.pending.clear()
        return 0


class FakeProc:
    def __init__(self, env: FakeEnv, run: int):
        self.env = env
        self.run = run
        self.returncode = None
        self.input_rows = 0
        self.batches = 0
        if env.crash_run == run:
            self.returncode = 1

    def step(self) -> None:
        env = self.env
        if self.returncode is not None:
            return
        batch = (max(env.commits) + 1) if env.commits else 0
        rows = env.queue[env.pos : env.pos + env.batch_size]
        env.offsets.add(batch)
        if batch not in env.written or not env.idempotent_sink:
            env.delta.extend(rows)
            env.written.add(batch)
        env.commits.add(batch)
        env.pos += len(rows)
        self.input_rows += len(rows)
        self.batches += 1
        if env.pos >= len(env.queue):
            self.returncode = 0

    def poll(self):
        return self.returncode

    def kill_hard(self):
        env = self.env
        if self.returncode is not None:
            return
        if env.kill_mid_batch:  # Delta written and offsets logged, commit lost
            batch = (max(env.commits) + 1) if env.commits else 0
            env.offsets.add(batch)
            env.delta.extend(env.queue[env.pos : env.pos + env.batch_size])
            env.written.add(batch)
        self.returncode = -9
        env.log.append(("kill-consumer", self.run))

    def wait(self, timeout=None):
        return self.returncode

    def metrics(self):
        return {"input_rows": self.input_rows, "micro_batches": self.batches, "seconds": 1.0}


class FakeEnv:
    def __init__(
        self,
        *,
        failover_polls: int = 2,
        resync_polls: int = 2,
        lose=(),
        replay=(),
        sink_dup: bool = False,
        ack_on_produce: bool = True,
        batch_size: int = 3,
        kill_mid_batch: bool = False,
        idempotent_sink: bool = True,
        crash_run: int | None = None,
    ):
        self.alive = {1, 2, 3}
        self.dead = None
        self.restarting = None
        self.failover_polls, self.resync_polls = failover_polls, resync_polls
        self._failover_left = self._resync_left = 0
        self.lose, self.replay = set(lose), set(replay)
        self.sink_dup = sink_dup
        self.ack_on_produce = ack_on_produce
        self.produced_count = 0
        self.topics: dict[str, dict[int, list[bytes]]] = {}
        self.delta: list[tuple[int, int, bytes]] = []
        self.log: list[tuple] = []
        # consumer
        self.batch_size = batch_size
        self.kill_mid_batch = kill_mid_batch
        self.idempotent_sink = idempotent_sink
        self.crash_run = crash_run
        self.queue: list[tuple[int, int, bytes]] = []
        self.pos = 0
        self.commits: set[int] = set()
        self.offsets: set[int] = set()
        self.written: set[int] = set()
        self.running: FakeProc | None = None

    # cluster
    def brokers(self):
        return frozenset(self.alive)

    def state(self, topic):
        n = len(self.topics[topic])
        if self.dead is not None and self._failover_left > 0:
            self._failover_left -= 1
            return fault.TopicState(
                brokers=frozenset({1, 2, 3}),
                partitions={
                    p: fault.PartitionState(leader=1 + p % 3, isr=(1, 2, 3)) for p in range(n)
                },
            )
        if self.restarting is not None:
            if self._resync_left > 0:
                self._resync_left -= 1
                lagging = self.restarting
            else:
                lagging = None
                self.restarting = None
        else:
            lagging = self.dead
        parts = {}
        for p in range(n):
            leader = 1 + p % 3
            if leader == self.dead:
                leader = min(self.alive)
            isr = tuple(b for b in (1, 2, 3) if b != lagging)
            parts[p] = fault.PartitionState(leader=leader, isr=isr)
        return fault.TopicState(brokers=frozenset(self.alive), partitions=parts)

    def create_topic(self, topic, partitions):
        self.log.append(("create", topic))
        self.topics[topic] = {p: [] for p in range(partitions)}

    def delete_topic(self, topic):
        self.log.append(("delete", topic))
        self.topics.pop(topic, None)

    def producer(self):
        return FakeProducer(self)

    def topic_messages(self, topic):
        return sum(len(v) for v in self.topics[topic].values())

    def kill_broker(self, broker):
        self.log.append(("kill", broker))
        self.alive.discard(broker)
        self.dead = broker
        self._failover_left = self.failover_polls

    def start_broker(self, broker):
        self.log.append(("start", broker))
        self.alive.add(broker)
        self.dead = None
        self.restarting = broker
        self._resync_left = self.resync_polls

    # sink
    def _rows(self, topic):
        return [
            (p, o, hashlib.sha256(v).digest())
            for p, values in sorted(self.topics[topic].items())
            for o, v in enumerate(values)
        ]

    def drain(self, topic, cell_dir):
        self.delta = self._rows(topic)
        if self.sink_dup:
            self.delta.append(self.delta[0])
        return {"seconds": 1.5, "micro_batches": 2, "input_rows": len(self.delta)}

    def sink(self, cell_dir):
        return fault.profile_rows(self.delta)

    def delta_rows(self, cell_dir):
        return len(self.delta)

    # consumer process
    def launch_consumer(self, topic, cell_dir, run):
        if not self.queue:
            self.queue = self._rows(topic)
        self.log.append(("launch", run))
        self.running = FakeProc(self, run)
        return self.running

    def committed(self, cell_dir):
        if self.running is not None:
            self.running.step()
        return set(self.commits)

    def offset_batches(self, cell_dir):
        return set(self.offsets)


def write_files(tmp_path: Path) -> list[Path]:
    lines = FIXTURE.read_bytes().splitlines()
    gz = tmp_path / f"{DATE}-0.json.gz"
    with gzip.open(gz, "wb") as fh:
        fh.write(b"\n".join(lines) + b"\n")
    return [gz]


def config(tmp_path: Path, clock: FakeClock, **overrides) -> streaming_fault.CaseConfig:
    files = write_files(tmp_path)
    values = {
        "data_date": DATE,
        "partitions": 3,
        "files": files,
        "source": fault.source_counts(files),
        "kill_at_fraction": 0.3,
        "kill_broker": None,
        "kill_after_batches": 2,
        "timeout": 60.0,
        "resync_timeout": 60.0,
        "interval": 0.5,
        "work_dir": tmp_path / "work",
        "clock": clock,
        "sleep": clock.sleep,
        "spawn": streaming_fault.run_inline,
    }
    values.update(overrides)
    return streaming_fault.CaseConfig(**values)


# ---------------------------------------------------------------- names and parsing


def test_variant_names():
    assert streaming_fault.variant("broker-kill") == "streaming-fault-broker-kill"
    assert streaming_fault.variant("consumer-kill") == "streaming-fault-consumer-kill"
    assert streaming_fault.CASES == ("broker-kill", "consumer-kill")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("broker-kill,consumer-kill", ["broker-kill", "consumer-kill"]),
        ("consumer-kill", ["consumer-kill"]),
        (" broker-kill ", ["broker-kill"]),
    ],
)
def test_parse_cases(text, expected):
    assert streaming_fault.parse_cases(text) == expected


@pytest.mark.parametrize("text", ["", "broker", "broker-kill,broker-kill", "broker-kill,,x"])
def test_parse_cases_rejects_bad_lists(text):
    with pytest.raises(ValueError):
        streaming_fault.parse_cases(text)


def test_broker_service_names_match_the_compose_node_ids():
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    for name, svc in data["services"].items():
        node = int(svc["environment"]["KAFKA_NODE_ID"])
        assert streaming_fault.broker_service(node) == name


def test_reuses_the_streaming_benchmark_cluster_constants():
    assert streaming_fault.KAFKA_IMAGE == streaming_bench.KAFKA_IMAGE
    assert streaming_fault.DEFAULT_BOOTSTRAP == streaming_bench.DEFAULT_BOOTSTRAP
    assert streaming_fault.COMPOSE_FILE == Path("benchmarks/kafka/docker-compose.yml")


def test_fault_topic_is_one_per_case():
    assert streaming_fault.fault_topic(DATE, 3, "broker-kill") == (
        "gharchive-2026-10-25-p3-broker-kill"
    )


def test_consumer_command_runs_the_consumer_module_with_the_same_checkpoint(tmp_path):
    cmd = streaming_fault.consumer_command(
        python="python3",
        bootstrap="localhost:19092",
        topic="t",
        cell_dir=tmp_path,
        max_offsets_per_trigger=50000,
        driver_memory="3g",
        metrics_out=tmp_path / "m.json",
    )
    assert cmd[:3] == ["python3", "-m", "repopulse.streaming.consumer"]
    joined = " ".join(cmd)
    assert f"--delta {tmp_path / 'delta'}" in joined
    assert f"--checkpoint {tmp_path / 'checkpoint'}" in joined
    assert "--max-offsets-per-trigger 50000" in joined
    assert "--driver-memory 3g" in joined
    assert "--topic t" in joined and "--bootstrap localhost:19092" in joined
    assert f"--metrics-out {tmp_path / 'm.json'}" in joined


# ---------------------------------------------------------------- broker-kill


def test_broker_kill_mid_production_measures_failover_and_resync(tmp_path):
    clock = FakeClock()
    env = FakeEnv(failover_polls=2, resync_polls=3)
    result = streaming_fault.run_broker_kill(env, config(tmp_path, clock))
    assert result.case == "broker-kill"
    assert result.problems == []
    assert result.recon.ok is True
    assert result.recon.source_rows == 20
    notes = result.notes
    assert notes["killed_broker"] == "kafka-1"  # leader of partition 0 in the fake
    assert notes["kill_threshold"] == 6
    assert notes["delivered_at_kill"] == 6
    assert notes["sent_at_kill"] == 6
    assert notes["failover_seconds"] == pytest.approx(1.0)  # two stale polls of 0.5 s
    assert notes["resync_seconds"] == pytest.approx(1.5)  # three lagging polls of 0.5 s
    assert notes["produced"] == 20
    assert notes["topic_messages"] == 20
    assert notes["consume_seconds"] == pytest.approx(1.5)
    assert notes["micro_batches_before_kill"] == 0
    assert notes["micro_batches_after_kill"] == 2
    for name in (
        "max_delivery_gap_after_kill_seconds",
        "max_delivery_gap_before_kill_seconds",
        "kill_at_seconds",
        "produce_seconds",
        "produce_failed",
        "undelivered",
        "kill_at_fraction",
    ):
        assert name in notes
    assert result.total_seconds == pytest.approx(notes["produce_seconds"] + 1.5)
    kinds = [e[0] for e in env.log]
    assert (
        kinds.index("create") < kinds.index("kill") < kinds.index("start") < kinds.index("delete")
    )
    assert env.alive == {1, 2, 3}
    assert not (tmp_path / "work" / "broker-kill").exists()


def test_broker_kill_can_target_a_chosen_broker(tmp_path):
    clock = FakeClock()
    env = FakeEnv()
    result = streaming_fault.run_broker_kill(env, config(tmp_path, clock, kill_broker=3))
    assert result.notes["killed_broker"] == "kafka-3"
    assert ("kill", 3) in env.log


def test_broker_kill_reports_a_lost_message_without_hiding_it(tmp_path):
    env = FakeEnv(lose={10})
    result = streaming_fault.run_broker_kill(env, config(tmp_path, FakeClock()))
    assert result.problems == []  # a valid measurement ...
    assert result.recon.lost == 1  # ... that shows a loss
    assert result.recon.duplicates == 0


def test_broker_kill_reports_a_producer_replay_as_content_duplicate(tmp_path):
    env = FakeEnv(replay={12})
    result = streaming_fault.run_broker_kill(env, config(tmp_path, FakeClock()))
    assert result.recon.content_duplicates == 1
    assert result.recon.offset_duplicates == 0
    assert result.recon.lost == 0


def test_broker_kill_reports_a_sink_duplicate_as_offset_duplicate(tmp_path):
    env = FakeEnv(sink_dup=True)
    result = streaming_fault.run_broker_kill(env, config(tmp_path, FakeClock()))
    assert result.recon.offset_duplicates == 1


def test_broker_kill_that_never_happens_is_invalid(tmp_path):
    env = FakeEnv(ack_on_produce=False)  # no delivery report before flush: threshold never met
    result = streaming_fault.run_broker_kill(env, config(tmp_path, FakeClock()))
    assert result.problems and "not killed" in result.problems[0]
    assert all(e[0] != "kill" for e in env.log)
    assert ("delete", "gharchive-2026-10-25-p3-broker-kill") in env.log


def test_broker_kill_after_the_last_line_is_invalid_and_the_broker_comes_back(tmp_path):
    env = FakeEnv()
    result = streaming_fault.run_broker_kill(
        env, config(tmp_path, FakeClock(), kill_at_fraction=1.0)
    )
    assert result.problems
    assert ("kill", 1) in env.log and ("start", 1) in env.log
    assert env.alive == {1, 2, 3}


def test_failover_timeout_fails_the_case_but_restarts_the_broker(tmp_path):
    env = FakeEnv(failover_polls=10_000)
    with pytest.raises(TimeoutError):
        streaming_fault.run_broker_kill(env, config(tmp_path, FakeClock(), timeout=5.0))
    assert ("start", 1) in env.log
    assert env.alive == {1, 2, 3}
    assert any(e[0] == "delete" for e in env.log)


def test_broker_kill_waits_for_three_brokers_before_starting(tmp_path):
    env = FakeEnv()
    env.alive = {1, 2}
    with pytest.raises(TimeoutError, match="broker"):
        streaming_fault.run_broker_kill(env, config(tmp_path, FakeClock(), timeout=2.0))
    assert all(e[0] != "create" for e in env.log)


# ---------------------------------------------------------------- consumer-kill


def test_consumer_kill_between_batches_resumes_from_the_checkpoint(tmp_path):
    clock = FakeClock()
    env = FakeEnv(batch_size=3)
    result = streaming_fault.run_consumer_kill(env, config(tmp_path, clock, kill_after_batches=2))
    assert result.case == "consumer-kill"
    assert result.problems == []
    assert result.recon.ok is True
    notes = result.notes
    assert notes["micro_batches_before_kill"] == 2
    assert notes["micro_batches_after_kill"] == 5  # 14 rows left, 3 per batch
    assert notes["rows_before_restart"] == 6
    assert notes["input_rows_after_restart"] == 14
    assert notes["uncommitted_batches_at_kill"] == 0
    assert notes["kill_after_batches"] == 2
    assert notes["restart_first_commit_seconds"] == pytest.approx(0.0)
    assert notes["restart_drain_seconds"] == pytest.approx(2.0)
    for name in ("run1_seconds", "run2_seconds", "produce_seconds", "produced", "topic_messages"):
        assert name in notes
    assert result.total_seconds == pytest.approx(
        notes["produce_seconds"] + notes["run1_seconds"] + notes["run2_seconds"]
    )
    assert [e for e in env.log if e[0] in ("launch", "kill-consumer")] == [
        ("launch", 1),
        ("kill-consumer", 1),
        ("launch", 2),
    ]
    assert ("delete", "gharchive-2026-10-25-p3-consumer-kill") in env.log
    assert not (tmp_path / "work" / "consumer-kill").exists()


def test_consumer_killed_mid_batch_replays_it_without_duplicates(tmp_path):
    env = FakeEnv(batch_size=3, kill_mid_batch=True)
    result = streaming_fault.run_consumer_kill(env, config(tmp_path, FakeClock()))
    assert result.problems == []
    assert result.notes["uncommitted_batches_at_kill"] == 1
    assert result.recon.ok is True


def test_a_non_idempotent_sink_shows_up_as_offset_duplicates(tmp_path):
    env = FakeEnv(batch_size=3, kill_mid_batch=True, idempotent_sink=False)
    result = streaming_fault.run_consumer_kill(env, config(tmp_path, FakeClock()))
    assert result.problems == []
    assert result.recon.offset_duplicates == 3
    assert result.recon.duplicates == 3
    assert result.recon.lost == 0


def test_consumer_that_drained_the_topic_before_the_kill_is_invalid(tmp_path):
    env = FakeEnv(batch_size=1000)  # one batch reads everything and the query stops
    result = streaming_fault.run_consumer_kill(
        env, config(tmp_path, FakeClock(), kill_after_batches=5)
    )
    assert result.problems and any("finished" in p for p in result.problems)
    assert ("launch", 2) not in env.log
    assert ("delete", "gharchive-2026-10-25-p3-consumer-kill") in env.log


def test_consumer_crash_before_the_kill_fails_the_case(tmp_path):
    env = FakeEnv(crash_run=1)
    with pytest.raises(streaming_fault.StreamingFaultError, match="exit"):
        streaming_fault.run_consumer_kill(env, config(tmp_path, FakeClock()))
    assert any(e[0] == "delete" for e in env.log)


# ---------------------------------------------------------------- rows


def parse_notes(notes: str) -> dict[str, str]:
    return dict(item.split("=", 1) for item in notes.split())


def ok_recon(**changes):
    values = {
        "source_rows": 1000,
        "sink_rows": 1000,
        "distinct_offsets": 1000,
        "lost": 0,
        "offset_duplicates": 0,
        "content_duplicates": 0,
        "unexpected": 0,
    }
    values.update(changes)
    return fault.ContentReconciliation(**values)


def case_result(case="broker-kill", recon=None, problems=None):
    notes = {"failover_seconds": 9.25, "resync_seconds": 4.5, "killed_broker": "kafka-2"}
    if case == "consumer-kill":
        notes = {"restart_first_commit_seconds": 12.0, "micro_batches_before_kill": 4}
    return streaming_fault.CaseResult(
        case=case,
        recon=recon if recon is not None else ok_recon(),
        total_seconds=33.5,
        notes=notes,
        problems=problems or [],
    )


def build(result):
    return streaming_fault.build_row(
        result,
        data_date=DATE,
        git_sha=SHA,
        run_at=RUN_AT,
        partitions=3,
        hours=24,
        max_offsets_per_trigger=50000,
    )


def test_build_row_is_a_valid_runs_csv_row_with_counts_and_recovery_times():
    row = build(case_result())
    checked = validate_row(row)
    assert checked["variant"] == "streaming-fault-broker-kill"
    assert checked["data_date"] == DATE
    assert checked["row_count"] == "1000"
    assert row["total_seconds"] == pytest.approx(33.5)
    notes = parse_notes(row["notes"])
    assert notes["case"] == "broker-kill"
    assert notes["lost"] == "0"
    assert notes["duplicates"] == "0"
    assert notes["offset_duplicates"] == "0"
    assert notes["content_duplicates"] == "0"
    assert notes["source_rows"] == "1000"
    assert notes["failover_seconds"] == "9.25"
    assert notes["resync_seconds"] == "4.5"
    assert notes["killed_broker"] == "kafka-2"
    assert notes["partitions"] == "3"
    assert notes["brokers"] == "3"
    assert notes["rf"] == "3"
    assert notes["min_isr"] == "2"
    assert notes["hours"] == "24"
    assert notes["max_offsets_per_trigger"] == "50000"
    assert notes["kafka"] == streaming_fault.KAFKA_IMAGE


def test_build_row_writes_none_for_a_missing_number():
    result = case_result()
    result.notes["max_delivery_gap_after_kill_seconds"] = None
    notes = parse_notes(build(result)["notes"])
    assert notes["max_delivery_gap_after_kill_seconds"] == "none"


def test_build_row_keeps_real_loss_and_duplicate_numbers():
    row = build(case_result(recon=ok_recon(sink_rows=1001, lost=2, content_duplicates=3)))
    notes = parse_notes(row["notes"])
    assert notes["lost"] == "2"
    assert notes["duplicates"] == "3"
    assert row["row_count"] == 1001


def test_build_row_refuses_an_invalid_measurement():
    with pytest.raises(streaming_fault.InvalidMeasurement):
        build(case_result(problems=["broker was not killed"]))


def read_rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def run_cases(tmp_path, run_one, cases=("broker-kill", "consumer-kill")):
    out, summary = tmp_path / "rows.csv", tmp_path / "summary.md"
    code = streaming_fault.run_cases(
        list(cases),
        run_one=run_one,
        data_date=DATE,
        git_sha=SHA,
        run_at=RUN_AT,
        out=out,
        summary=summary,
        partitions=3,
        hours=24,
        max_offsets_per_trigger=50000,
    )
    return code, out, summary


def test_run_cases_writes_one_row_per_valid_case(tmp_path):
    code, out, summary = run_cases(tmp_path, lambda case: case_result(case))
    assert code == 0
    assert out.read_text(encoding="utf-8").splitlines()[0] == ",".join(COLUMNS)
    assert [r["variant"] for r in read_rows(out)] == [
        "streaming-fault-broker-kill",
        "streaming-fault-consumer-kill",
    ]
    text = summary.read_text(encoding="utf-8")
    assert "streaming-fault-broker-kill" in text and "streaming-fault-consumer-kill" in text


def test_a_violation_is_recorded_with_its_numbers_and_exits_1(tmp_path, capsys):
    bad = ok_recon(sink_rows=999, lost=1)

    def run_one(case):
        return case_result(case, recon=bad if case == "consumer-kill" else None)

    code, out, summary = run_cases(tmp_path, run_one)
    assert code == 1
    rows = read_rows(out)
    assert [r["variant"] for r in rows] == [
        "streaming-fault-broker-kill",
        "streaming-fault-consumer-kill",
    ]
    assert parse_notes(rows[1]["notes"])["lost"] == "1"
    assert "VIOLATION" in summary.read_text(encoding="utf-8")
    assert "WARNING" in capsys.readouterr().err


def test_an_invalid_measurement_writes_no_row_and_exits_1(tmp_path):
    def run_one(case):
        if case == "broker-kill":
            return case_result(case, problems=["broker killed after every line was sent"])
        return case_result(case)

    code, out, summary = run_cases(tmp_path, run_one)
    assert code == 1
    assert [r["variant"] for r in read_rows(out)] == ["streaming-fault-consumer-kill"]
    text = summary.read_text(encoding="utf-8")
    assert "INVALID" in text and "every line was sent" in text


def test_a_crashing_case_does_not_stop_the_other(tmp_path):
    def run_one(case):
        if case == "broker-kill":
            raise TimeoutError("ISR not back to 3")
        return case_result(case)

    code, out, summary = run_cases(tmp_path, run_one)
    assert code == 1
    assert [r["variant"] for r in read_rows(out)] == ["streaming-fault-consumer-kill"]
    assert "ISR not back to 3" in summary.read_text(encoding="utf-8")


# ---------------------------------------------------------------- CLI


def test_main_requires_git_sha_before_touching_kafka(capsys):
    assert streaming_fault.main(["--cases", "broker-kill"]) == 2
    assert "git-sha" in capsys.readouterr().err


@pytest.mark.parametrize(
    "args",
    [
        ["--cases", "nope"],
        ["--kill-at-fraction", "0"],
        ["--kill-at-fraction", "1.5"],
        ["--kill-after-batches", "0"],
        ["--kill-broker", "4"],
        ["--max-hours", "0"],
        ["--partitions", "0"],
        ["--max-offsets-per-trigger", "0"],
    ],
)
def test_main_rejects_bad_arguments(args):
    with pytest.raises(SystemExit) as exc:
        streaming_fault.main(["--git-sha", SHA, *args])
    assert exc.value.code == 2


def test_main_fails_when_reference_files_are_missing(tmp_path, capsys):
    code = streaming_fault.main(["--git-sha", SHA, "--data-dir", str(tmp_path), "--max-hours", "2"])
    assert code == 1
    assert "missing" in capsys.readouterr().err


def test_defaults():
    assert streaming_fault.DEFAULT_CASES == "broker-kill,consumer-kill"
    assert streaming_fault.DEFAULT_PARTITIONS == 3
    assert 0 < streaming_fault.DEFAULT_KILL_AT_FRACTION < 1
    assert streaming_fault.DEFAULT_KILL_AFTER_BATCHES >= 1
    assert (
        streaming_fault.DEFAULT_MAX_OFFSETS_PER_TRIGGER
        < streaming_bench.DEFAULT_MAX_OFFSETS_PER_TRIGGER
    )
