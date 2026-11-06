"""Part C.3 -- key skew study"""
import argparse
import json
import statistics
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark_jobs.batch_common import (
    aggregate_plain, aggregate_salted, create_batch_session, join_profile, join_profile_salted, read_day,
    vehicle_profile,
)


def rest(spark, path):
    url = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}/{path}"
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def stage_task_stats(spark, group):
    """Task-time statistics of every multi-task stage of the jobs run under `group`"""
    stats = []
    jobs = [job for job in rest(spark, "jobs") if job.get("jobGroup") == group]
    for job in sorted(jobs, key=lambda j: j["jobId"])[-1:]:
        for stage_id in job["stageIds"]:
            attempts = rest(spark, f"stages/{stage_id}")
            stage = attempts[-1]
            if stage["status"] != "COMPLETE" or stage["numTasks"] < 2:
                continue
            summary = rest(spark, f"stages/{stage_id}/{stage['attemptId']}/taskSummary?quantiles=0.0,0.5,1.0")
            run = summary["executorRunTime"]
            records = summary["inputMetrics"]["recordsRead"], summary["shuffleReadMetrics"]["readRecords"]
            stats.append({
                "stage_id": stage_id,
                "name": stage["name"][:60],
                "tasks": stage["numTasks"],
                "task_ms_min": run[0], "task_ms_median": run[1], "task_ms_max": run[2],
                "max_over_median": round(run[2] / run[1], 2) if run[1] else None,
                "records_read_max": max(records[0][2], records[1][2]),
                "records_read_median": max(records[0][1], records[1][1]),
            })
    return stats


def run_config(spark, name, build, repeats):
    """Runs the DataFrame produced by build() `repeats` times (plus one warm-up) with the noop sink"""
    group = f"skew-{name}"
    build().write.format("noop").mode("overwrite").save()
    times = []
    spark.sparkContext.setJobGroup(group, name)
    for _ in range(repeats):
        start = time.time()
        build().write.format("noop").mode("overwrite").save()
        times.append(round(time.time() - start, 2))
    return {"config": name, "seconds_median": statistics.median(times), "seconds_all": times,
            "stages": stage_task_stats(spark, group)}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--events-path", required=True)
    parser.add_argument("--output", default="experiments/skew_study.json")
    parser.add_argument("--salt-buckets", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--keep-ui", type=int, default=0, help="seconds to keep the Spark UI alive at the end")
    args = parser.parse_args(argv)

    spark = create_batch_session("FleetSkewStudy", shuffle_partitions=args.shuffle_partitions, aqe=False, extra_conf={
        "spark.ui.retainedJobs": "1000", "spark.ui.retainedStages": "5000",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "8388608",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "8388608",
        "spark.sql.adaptive.coalescePartitions.enabled": "false"})
    results = []
    try:
        events = read_day(spark, args.events_path, args.date).select("event_id", "vehicle_id", "speed_kmh", "harsh_braking", "region")
        events = events.repartition(args.shuffle_partitions).cache()
        total = events.count()
        hot = events.filter(events.vehicle_id == "V0001").count()
        print(f"{total} rows, V0001 share = {100 * hot / total:.1f}%", flush=True)
        profile = vehicle_profile(spark).cache()
        profile.count()

        n = args.salt_buckets
        configs = [
            ("aggregate_plain_aqe_off", False, False, True, lambda: aggregate_plain(events, "vehicle_id")),
            ("aggregate_plain_aqe_on", True, False, True, lambda: aggregate_plain(events, "vehicle_id")),
            (f"aggregate_salted{n}_aqe_off", False, False, True, lambda: aggregate_salted(events, "vehicle_id", n)),
            ("join_broadcast_aqe_off", False, False, True, lambda: join_profile(events, profile)),
            ("join_smj_aqe_off", False, False, False, lambda: join_profile(events, profile)),
            ("join_smj_aqe_on_no_skew_handling", True, False, False, lambda: join_profile(events, profile)),
            ("join_smj_aqe_skew_join_factor5", True, True, False, lambda: join_profile(events, profile)),
            ("join_smj_aqe_skew_join_factor2", True, True, False, lambda: join_profile(events, profile), 2),
            (f"join_smj_salted{n}_aqe_off", False, False, False, lambda: join_profile_salted(events, profile, n)),
        ]
        for name, aqe, skew_join, broadcast, build, *factor in configs:
            spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", str(factor[0] if factor else 5))
            spark.conf.set("spark.sql.adaptive.enabled", "true" if aqe else "false")
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true" if skew_join else "false")
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760" if broadcast else "-1")
            result = run_config(spark, name, build, args.repeats)
            result.update({"aqe": aqe, "skew_join_handling": skew_join, "broadcast_allowed": broadcast,
                           "rows": total, "hot_share_pct": round(100 * hot / total, 1)})
            results.append(result)
            heavy = max(result["stages"], key=lambda s: s["task_ms_max"], default=None)
            detail = f"slowest stage: {heavy['tasks']} tasks, max {heavy['task_ms_max']:.0f} ms, median {heavy['task_ms_median']:.0f} ms" if heavy else ""
            print(f"{name:36s} median {result['seconds_median']:6.2f}s   {detail}", flush=True)

        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(results, indent=2) + "\n")
        print(f"results written to {out}", flush=True)
        if args.keep_ui:
            print(f"Spark UI at {spark.sparkContext.uiWebUrl} for {args.keep_ui}s", flush=True)
            time.sleep(args.keep_ui)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
