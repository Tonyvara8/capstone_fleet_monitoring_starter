# Part E -- experiments: protocol, hypotheses, evidence

**Hypotheses below were written on 2026-11-05 BEFORE any of these experiments was run.** Do not edit them after running:
write what you observe in the "Observed" lines of `report/report.tex` (and keep the raw outputs in the folders listed here).
If an observation contradicts a hypothesis, say so in the report: that is what is graded ("hypotheses before results, honest limits").

Everything needs the full stack up: `docker compose up -d`, topics created, catalog published, a clean `data/` and `checkpoints/`
(delete both folders before E1 so the counts start from zero), simulator at its default rate (20 events/s).

| Folder | Content to keep |
|---|---|
| `E1_kill9/` | `before_kill.json`, `after_restart.json` (from `scripts/count_outputs.py`), `streaming_job.log`, Spark UI screenshot |
| `E2_notifier_stop/` | notifier logs, the `kafka_lag_monitor` DAG run list (Airflow UI screenshot), lag values (task logs of `measure_lag`) |
| `E3_broker_stop/` | logs of the simulator, the Spark job and the notifier around the outage |
| `E4_burst/` | `progress.jsonl` of the run, Spark UI screenshot (Structured Streaming tab) |
| `perf/` | one JSON per run (`scripts/measure_run.py`), the progress logs, the filled table |
| `spark_ui/`, `airflow_ui/` | screenshots (stage page with task-time distribution, SQL tab with PartitionFilters, DAG graph and grid views) |
| `sandbox_runs/` | evidence produced while developing, WITHOUT Kafka (see its own README). It does not replace your runs above. |

---------------------------------------------------------------------------------------------------------------------------------

## E1 -- `kill -9` of the streaming job

**Hypothesis.** Sources of loss: none, because offsets are in the checkpoint and the Kafka log keeps the data. Parquet: no
duplicates and no loss, because the replayed micro-batch has the same `batch_id` and `partitionOverwriteMode=dynamic` rewrites exactly the
`batch_id=N` directories. Alerts: duplicated for at most one micro-batch (the kill can fall after the publication to `alerts` and
before the offsets are committed to the checkpoint): same `alert_id` twice, about `maxOffsetsPerTrigger x 3 %` = up to 30 messages.
`vehicle_events_dlq` can contain the same duplicates for the same reason. Everything else (windows, dedup state) is restored from the checkpoint.

```bash
# terminal 1: simulator (default rate), terminal 2: streaming job started with --progress-log experiments/E1_kill9/progress.jsonl
python scripts/count_outputs.py --label E1_kill9/before_kill        # let it run ~3 minutes first
kill -9 $(pgrep -f fleet_streaming.py)  # also kills the JVM child; check with `pgrep -f SparkSubmit`
# restart the SAME command (same checkpoints), wait ~2 minutes, then stop the simulator and wait for the job to catch up
python scripts/count_outputs.py --label E1_kill9/after_restart
python scripts/count_outputs.py --diff experiments/E1_kill9/before_kill.json experiments/E1_kill9/after_restart.json
```
Look for: `parquet_duplicate_rows` (expected 0), `alerts_duplicated_messages` (expected between 0 and ~30), `parquet_rows` growth.

## E2 -- notifier stopped for 10 minutes

**Hypothesis.** The group offsets are retained, so nothing is lost. At ~20 events/s the alerts topic gets ~0.6 messages/s, i.e. ~0.2/s per partition:
the lag grows linearly to ~120 per partition after 10 minutes. `kafka_lag_monitor` (threshold 50) publishes its first `consumer_lag_high`
after ~4-5 minutes (first 5-minute run above 50) and again every 5 minutes while the lag stays above the threshold. After the restart the
notifier commits synchronously after every message (a few ms), so it catches up within about a minute, and the lag DAG returns to `lag_ok`. The
restarted notifier also receives the `consumer_lag_high` alerts written to `alerts` during the outage.

```bash
# notifier running, lag DAG enabled (airflow dags unpause kafka_lag_monitor), simulator + streaming job running
# t0: stop the notifier (Ctrl+C). Note the time. Wait 10 minutes. Restart it. Watch the lag for 3 minutes.
airflow tasks test kafka_lag_monitor measure_lag $(date -u +%Y-%m-%dT%H:%M:%S)        # any time: prints the lag per partition
```

## E3 -- Kafka broker stopped for 1 minute

**Hypothesis.** Simulator (idempotent producer, `delivery.timeout.ms` default 120 s): the 1200 messages produced during the outage stay in the local queue
and are delivered after the restart, in order, with no loss and no duplicate; the end-of-run summary says `0 failed`. Spark: the Kafka source cannot
fetch offsets, the micro-batch fails and, in `local[2]` where `spark.task.maxFailures` is 1, the query most likely terminates with an exception instead of waiting
for the broker: restarting it resumes from the checkpoint without loss. The notifier (librdkafka reconnects by itself) logs transport errors, keeps running, but may
lose its group membership (session timeout 45 s): the next `commit` can then fail and stop the process; restarting it re-reads the uncommitted alert (at-least-once).
Settings that control this: producer `delivery.timeout.ms`, `queue.buffering.max.*`; Spark `kafka.request.timeout.ms`, `kafka.default.api.timeout.ms`,
`spark.task.maxFailures`, `failOnDataLoss`; consumer `session.timeout.ms`, `max.poll.interval.ms`.

```bash
docker stop capstone-kafka; sleep 60; docker start capstone-kafka     # simulator, Spark job and notifier running, logs redirected to E3_broker_stop/
```

## E4 -- burst at ten times the normal rate

**Hypothesis.** Normal rate = 20 events/s, burst = 200 events/s for 3 minutes (`scripts/load_generator.py --rate 200 --seconds 180`). The configuration has
`maxOffsetsPerTrigger=1000` every 5 s, which caps the source at 200 events/s: the burst saturates it exactly, so the consumer lag of the streaming query grows and
the end-to-end latency increases steadily. The bottleneck is the Spark micro-batch (the `addBatch` duration of the Parquet foreachBatch, a Parquet write per micro-batch
from a 2-core laptop), not Kafka and not the notifier (alerts at 200 events/s = 6 messages/s). First change: raise `maxOffsetsPerTrigger`; second:
fewer, bigger Parquet files (`coalesce`) and fewer shuffle partitions; third: more cores.

```bash
python scripts/load_generator.py --topic vehicle_events --rate 200 --seconds 180      # on top of / instead of the simulator
# keep Spark started with --progress-log experiments/E4_burst/progress.jsonl, then compare inputRowsPerSecond / processedRowsPerSecond / durationMs
```

---------------------------------------------------------------------------------------------------------------------------------

## Performance study

**Parameters varied:** `maxOffsetsPerTrigger` (1000 / 10000), `spark.sql.shuffle.partitions` (8 / 200), trigger interval (2 s / 5 s).
`vehicle_events` partitions (3 / 6) is optional (one more topic per value, `scripts/create_perf_topic.py`).

**Hypothesis.** (1) Throughput is capped by `maxOffsetsPerTrigger / trigger interval` (200 events/s for 1000 per 5 s): 10000 removes the cap, after which the
CPU of the machine is the limit. (2) `shuffle.partitions=200` is slower than 8 for 40 vehicles: every stateful operator keeps 200 state partitions that are
opened at each micro-batch, so batch duration grows and so does latency. (3) A 2 s trigger lowers the latency of the point alerts by about 1.5 s but
makes more, smaller batches (more small Parquet files). (4) Recommended configuration: `maxOffsetsPerTrigger=10000`, `shuffle.partitions=8`, trigger 5 s -- the
cheapest setting that never throttles the source, with the latency still in the single-digit seconds.

**Method (per configuration, run by `scripts/run_perf_study.py`).**
1. Throughput: a backlog of 100 000 events (burst, `load_generator.py --rate 0 --count 100000`) is written to a fresh topic *before* the Spark job
   starts; the job drains it; throughput = sum of `numInputRows` / sum of micro-batch durations of the Parquet query (from `progress.jsonl`).
2. Latency: the job keeps running while `load_generator.py --rate 100 --seconds 120` produces a steady load; latency of an alert = Kafka timestamp of the alert
   minus the `event_time` of its event (`scripts/measure_run.py`), p50 and p95. Producer and Spark are on the same machine, so the clocks agree.

```bash
python scripts/run_perf_study.py --configs all          # ~30-40 minutes, writes experiments/perf/*.json and experiments/perf/table.md
```
The result table is copied into the report; a recommendation that differs from the hypothesis above is a valid finding.
