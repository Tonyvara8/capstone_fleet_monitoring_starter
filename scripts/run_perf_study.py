"""Part E performance study: runs every configuration and writes experiments/perf/table.md"""
import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

CONFIGS = {
    "A": (1000, 8, 5),
    "B": (10000, 8, 5),
    "C": (1000, 200, 5),
    "D": (10000, 200, 5),
    "E": (10000, 8, 2),
}


def run(cmd, **kwargs):
    return subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=True, **kwargs)


def events_progress(path):
    rows = []
    if Path(path).exists():
        for line in Path(path).read_text().splitlines():
            try:
                progress = json.loads(line)
            except ValueError:
                continue
            if progress.get("name") == "events":
                rows.append(progress)
    return rows


def wait_for_backlog(progress_path, expected_rows, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        processed = sum(p.get("numInputRows", 0) for p in events_progress(progress_path))
        if processed >= expected_rows:
            return processed
        time.sleep(5)
    raise TimeoutError(f"backlog not drained after {timeout}s ({processed}/{expected_rows} rows)")


def throughput(progress_path):
    """Rows per second of micro-batch execution time, over the batches that read data"""
    rows = seconds = 0
    for p in events_progress(progress_path):
        if p.get("numInputRows", 0) > 0:
            rows += p["numInputRows"]
            seconds += p["durationMs"]["triggerExecution"] / 1000
    return round(rows / seconds, 1) if seconds else None


def stop(process):
    if os.name == "nt":
        # Windows has no process groups to signal: taskkill /T also ends the Java child of spark-submit.cmd
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(process.pid)], capture_output=True)
        process.wait(timeout=60)
        return
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGINT)
        process.wait(timeout=60)
    except Exception:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)


def run_config(name, backlog, steady_rate, steady_seconds, python, spark_submit, bootstrap):
    max_offsets, shuffle, trigger = CONFIGS[name]
    tag = f"perf_{name}_{int(time.time())}"
    topic = tag
    out_dir = ROOT / "experiments" / "perf"
    out_dir.mkdir(parents=True, exist_ok=True)
    progress = out_dir / f"{name}_progress.jsonl"
    progress.unlink(missing_ok=True)
    print(f"== config {name}: maxOffsetsPerTrigger={max_offsets} shuffle.partitions={shuffle} trigger={trigger}s (topic {topic})", flush=True)

    run([python, "scripts/create_perf_topic.py", "--partitions", "6", "--name", topic, "--bootstrap-servers", bootstrap])
    sent = int(re.search(r"sent (\d+) events", run([python, "scripts/load_generator.py", "--topic", topic, "--rate", "0",
                                                      "--count", str(backlog), "--bootstrap-servers", bootstrap]).stdout).group(1))

    job = subprocess.Popen(
        [spark_submit, "--master", "local[2]", "--packages", PACKAGE, "spark_jobs/fleet_streaming.py",
         "--input-topic", topic, "--run-tag", tag, "--bootstrap-servers", bootstrap,
         "--max-offsets-per-trigger", str(max_offsets), "--shuffle-partitions", str(shuffle),
         "--trigger-seconds", str(trigger), "--progress-log", str(progress)],
        cwd=ROOT, stdout=open(out_dir / f"{name}_job.log", "w"), stderr=subprocess.STDOUT, start_new_session=(os.name != "nt"))
    try:
        drained = wait_for_backlog(progress, sent, timeout=max(600, 3 * sent / max_offsets * trigger))
        result = {"config": name, "max_offsets_per_trigger": max_offsets, "shuffle_partitions": shuffle,
                  "trigger_seconds": trigger, "backlog_events": sent, "rows_drained": drained,
                  "throughput_events_per_s": throughput(progress)}

        generator = run([python, "scripts/load_generator.py", "--topic", topic, "--rate", str(steady_rate),
                         "--seconds", str(steady_seconds), "--bootstrap-servers", bootstrap]).stdout
        since_ms = int(re.search(r"started_at_ms=(\d+)", generator).group(1))
        time.sleep(30)
        latency_file = out_dir / f"{name}_latency.json"
        run([python, "scripts/measure_run.py", "--since-ms", str(since_ms), "--output", str(latency_file),
             "--bootstrap-servers", bootstrap])
        result.update(json.loads(latency_file.read_text()))
    finally:
        stop(job)
    (out_dir / f"{name}.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)
    return result


def write_table(results):
    lines = ["| Config | maxOffsetsPerTrigger | shuffle.partitions | trigger (s) | throughput (events/s) | latency p50 (s) | latency p95 (s) |",
             "|---|---|---|---|---|---|---|"]
    for r in results:
        lines.append(f"| {r['config']} | {r['max_offsets_per_trigger']} | {r['shuffle_partitions']} | {r['trigger_seconds']} | "
                     f"{r['throughput_events_per_s']} | {r['latency_p50_s']} | {r['latency_p95_s']} |")
    table = "\n".join(lines) + "\n"
    (ROOT / "experiments" / "perf" / "table.md").write_text(table)
    print(table)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--configs", default="all", help="comma-separated names among " + ",".join(CONFIGS) + ", or 'all'")
    parser.add_argument("--backlog", type=int, default=30000)
    parser.add_argument("--steady-rate", type=int, default=100)
    parser.add_argument("--steady-seconds", type=int, default=120)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--spark-submit", default="spark-submit.cmd" if os.name == "nt" else "spark-submit")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    args = parser.parse_args()
    names = list(CONFIGS) if args.configs == "all" else args.configs.split(",")
    results = [run_config(n, args.backlog, args.steady_rate, args.steady_seconds, args.python, args.spark_submit,
                          args.bootstrap_servers) for n in names]
    write_table(results)


if __name__ == "__main__":
    main()
