"""Checks that the project contains everything Section 8 of the subject asks for, then builds NAME1_NAME2_capstone.zip"""
import argparse
import re
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXCLUDE_DIRS = {".venv", ".venv-airflow", ".airflow", "__pycache__", "checkpoints", "data", ".git", "spark-warehouse", "metastore_db"}

REQUIRED = {
    "README with start commands": "README.md",
    "docker compose file": "docker-compose.yml",
    "Part A: catalog publisher": "producers/publish_fleet_catalog.py",
    "Part A: alerts notifier": "Alerts/alerts_notifier.py",
    "Part B: streaming job": "spark_jobs/fleet_streaming.py",
    "Part C: quality job": "spark_jobs/quality_job.py",
    "Part C: daily report job": "spark_jobs/daily_report_job.py",
    "Part C: skew study": "spark_jobs/skew_study.py",
    "Part D: fleet_daily_pipeline": "airflow_dags/fleet_daily_pipeline.py",
    "Part D: kafka_lag_monitor": "airflow_dags/kafka_lag_monitor.py",
    "Part E: PDF report": "report/report.pdf",
    "Contribution statements": "CONTRIBUTIONS.md",
}
PLACEHOLDER = re.compile(r"\[(LAST NAME|TEAMMATE[^\]]*|\.\.\.|Add:[^\]]*)\]|TO FILL|TODO FILL", re.I)


def pdf_pages(path):
    return len(re.findall(rb"/Type\s*/Page[^s]", path.read_bytes()))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", metavar="NAME1_NAME2", help="build ../NAME1_NAME2_capstone.zip")
    args = parser.parse_args()
    problems, warnings = [], []

    for label, rel in REQUIRED.items():
        print(("ok      " if (ROOT / rel).exists() else "MISSING ") + f"{label}: {rel}")
        if not (ROOT / rel).exists():
            problems.append(f"missing {rel}")

    report = ROOT / "report" / "report.pdf"
    if report.exists():
        pages = pdf_pages(report)
        print(f"        report has {pages} pages (maximum 10)")
        if pages > 10:
            problems.append("report longer than 10 pages")

    experiments = ROOT / "experiments"
    for folder, what in [("E1_kill9", "E1 raw outputs"), ("E2_notifier_stop", "E2 raw outputs"), ("E3_broker_stop", "E3 raw outputs"),
                         ("E4_burst", "E4 raw outputs"), ("perf", "performance study"), ("spark_ui", "Spark UI screenshots"),
                         ("airflow_ui", "Airflow UI screenshots")]:
        files = [p for p in (experiments / folder).rglob("*") if p.is_file() and not p.name.startswith(".")] if (experiments / folder).exists() else []
        print(("ok      " if files else "EMPTY   ") + f"experiments/{folder} ({what}): {len(files)} files")
        if not files:
            problems.append(f"experiments/{folder} is empty: run the experiment and keep its raw output")

    for rel in ["CONTRIBUTIONS.md", "report/report.tex"]:
        path = ROOT / rel
        if path.exists() and PLACEHOLDER.search(path.read_text()):
            problems.append(f"{rel} still contains placeholders ({PLACEHOLDER.search(path.read_text()).group(0)!r})")

    print()
    for message in warnings:
        print("WARNING", message)
    for message in problems:
        print("TO DO  ", message)
    if problems:
        print(f"\n{len(problems)} thing(s) to finish before submitting.")
    else:
        print("\nEverything the subject asks for is in the project.")

    if args.zip:
        out = ROOT.parent / f"{args.zip}_capstone.zip"
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(ROOT.rglob("*")):
                if path.is_file() and not (set(path.relative_to(ROOT).parts) & EXCLUDE_DIRS) and path.suffix != ".pyc":
                    archive.write(path, Path(args.zip + "_capstone") / path.relative_to(ROOT))
        print(f"written {out}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
