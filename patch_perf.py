from pathlib import Path

p = Path("scripts/run_perf_study.py")
s = p.read_text(encoding="utf-8")
old_stop = '''def stop(process):
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGINT)
        process.wait(timeout=60)
    except Exception:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
'''
new_stop = '''def stop(process):
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
'''
replacements = [
    (old_stop, new_stop),
    ("start_new_session=True)", 'start_new_session=(os.name != "nt"))'),
    ('default="spark-submit")', 'default="spark-submit.cmd" if os.name == "nt" else "spark-submit")'),
]
for old, new in replacements:
    assert s.count(old) == 1, f"pattern not found exactly once: {old[:40]}"
    s = s.replace(old, new)
p.write_text(s, encoding="utf-8")
print("patched")