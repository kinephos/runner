# runner.py
#!/usr/bin/env python3
import argparse
import logging
import os
import pathlib
import signal
import subprocess
import sys
from logging.handlers import RotatingFileHandler

import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone

DEFAULT_TZ = "America/New_York"
def safe_config_path(raw_path: str) -> Path:
    """
    Resolve a config path and ensure it stays under an allowed base dir.
    Raises ValueError if it escapes.
    """
    # Base directory for all runner configs; adjust as you like
    base = Path(os.environ.get("RUNNER_CONFIG_BASE", "/etc/python-runner")).expanduser()
    base_resolved = base.resolve(strict=False)

    p = Path(raw_path).expanduser()
    resolved = p.resolve(strict=False)

    # Ensure resolved is base or inside base
    if resolved == base_resolved or base_resolved in resolved.parents:
        return resolved

    raise ValueError(f"Unsafe config path outside base dir: {resolved}")


def setup_logging(logfile):
    logger = logging.getLogger("runner")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(logfile, maxBytes=10_000_000, backupCount=5)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)
    return logger


def run_job(cmd, env, logger):
    logger.info(f"START: {cmd}")
    try:
        cp = subprocess.run(cmd, shell=True, check=False, capture_output=True, text=True, env=env)
        logger.info(f"EXIT {cp.returncode}: {cmd}")
        if cp.stdout:
            logger.info(f"STDOUT:\n{cp.stdout.strip()}")
        if cp.stderr:
            logger.warning(f"STDERR:\n{cp.stderr.strip()}")
    except Exception as e:
        logger.exception(f"Job failed: {cmd} :: {e}")


def load_jobs(path):
    cfg_path = safe_config_path(str(path))
    with cfg_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("jobs", [])

# def load_jobs(path):
#     with open(path, "r", encoding="utf-8") as f:
#         data = yaml.safe_load(f) or {}
#     return data.get("jobs", [])


def main():
    ap = argparse.ArgumentParser(
        description="Single-process task scheduler (cron-like) for Python/tools."
    )
    ap.add_argument("-c", "--config", required=True, help="YAML file with jobs")
    ap.add_argument("--log", default="/var/log/python-runner.log", help="Log file path")
    ap.add_argument("--tz", default=DEFAULT_TZ, help="Default timezone for jobs")
    ap.add_argument(
        "--env", action="append", default=[], help="Extra env like KEY=VALUE (can repeat)"
    )
    args = ap.parse_args()

    # Logging
    pathlib.Path(os.path.dirname(args.log) or ".").mkdir(parents=True, exist_ok=True)
    logger = setup_logging(args.log)
    logger.info("Python Runner starting")

    # Base environment
    base_env = os.environ.copy()
    for kv in args.env:
        k, _, v = kv.partition("=")
        base_env[k] = v

    # Scheduler
    sched = BlockingScheduler(timezone=timezone(args.tz))

    # Load jobs
    for job in load_jobs(args.config):
        name = job["name"]
        cmd = job["cmd"]
        spec = job["when"]  # standard 5-field crontab string (min hour day month dow)
        jtz = job.get("timezone") or args.tz
        env = base_env.copy()
        for kv in job.get("env", []):
            k, _, v = kv.partition("=")
            env[k] = v

        trigger = CronTrigger.from_crontab(spec, timezone=timezone(jtz))
        sched.add_job(
            run_job,
            trigger=trigger,
            args=[cmd, env, logger],
            id=name,
            name=name,
            replace_existing=True,
            max_instances=1,  # prevent overlap per job
            coalesce=True,  # catch-up collapse
            misfire_grace_time=300,
        )
        logger.info(f"JOB LOADED: {name} @ {spec} [{jtz}] -> {cmd}")

    # Graceful shutdown
    def _stop(signum, frame):
        logger.info(f"Received signal {signum}, shutting down…")
        sched.shutdown(wait=False)
        sys.exit(0)

    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, _stop)

    logger.info("Scheduler started")
    sched.start()


if __name__ == "__main__":
    main()
