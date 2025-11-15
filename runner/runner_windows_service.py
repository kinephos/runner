# runner_windows_service.py
#!/usr/bin/env python3
import logging
import os
import pathlib
import subprocess
import sys
from logging.handlers import RotatingFileHandler

import servicemanager

# --- Windows service bits ---
import win32event
import win32service
import win32serviceutil
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone

DEFAULT_TZ = "America/New_York"

def safe_config_path(raw_path: str) -> Path:
    """
    Resolve a config path and ensure it stays under an allowed base dir.
    Raises ValueError if it escapes.
    """
    # Base directory for all runner configs; adjust as you like
    base = Path(os.environ.get("RUNNER_CONFIG_BASE", r"C:\etc\python-runner")).expanduser()
    base_resolved = base.resolve(strict=False)

    p = Path(raw_path).expanduser()
    resolved = p.resolve(strict=False)

    # Ensure resolved is base or inside base
    if resolved == base_resolved or base_resolved in resolved.parents:
        return resolved

    raise ValueError(f"Unsafe config path outside base dir: {resolved}")

def setup_logging(logfile):
    pathlib.Path(os.path.dirname(logfile) or ".").mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("runner")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(logfile, maxBytes=10_000_000, backupCount=5)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(console)
    return logger


def filename_norm(p):
    return os.path.normpath(os.path.expandvars(os.path.expanduser(p)))

def load_jobs(path):
    cfg_path = safe_config_path(str(path))
    with cfg_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("jobs", [])

# def load_jobs(path):
#     with open(path, "r", encoding="utf-8") as f:
#         data = yaml.safe_load(f) or {}
#     return data.get("jobs", [])


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


class PythonRunnerService(win32serviceutil.ServiceFramework):
    _svc_name_ = "PythonRunner"
    _svc_display_name_ = "Python Runner (Single-process scheduler)"
    _svc_description_ = "Runs scheduled commands from a YAML config using APScheduler."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        # Config via env or defaults
        self.config_path = filename_norm(
            os.environ.get("RUNNER_CONFIG", r"C:\etc\python-runner\jobs.yaml")
        )
        self.log_path = filename_norm(os.environ.get("RUNNER_LOG", r"C:\logs\python-runner.log"))
        self.default_tz = os.environ.get("RUNNER_TZ", DEFAULT_TZ)
        self.base_env = os.environ.copy()
        self.logger = setup_logging(self.log_path)
        self.scheduler = BackgroundScheduler(timezone=timezone(self.default_tz))

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        try:
            self.logger.info("Service stop requested.")
            self.scheduler.shutdown(wait=False)
        except Exception:
            pass
        win32event.SetEvent(self.stop_event)

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, ""),
        )
        self.logger.info("Python Runner service starting")
        try:
            jobs = load_jobs(self.config_path)
            for job in jobs:
                name = job["name"]
                cmd = job["cmd"]
                spec = job["when"]
                jtz = job.get("timezone") or self.default_tz
                env = self.base_env.copy()
                for kv in job.get("env", []):
                    k, _, v = kv.partition("=")
                    env[k] = v
                trigger = CronTrigger.from_crontab(spec, timezone=timezone(jtz))
                self.scheduler.add_job(
                    run_job,
                    trigger=trigger,
                    args=[cmd, env, self.logger],
                    id=name,
                    name=name,
                    replace_existing=True,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300,
                )
                self.logger.info(f"JOB LOADED: {name} @ {spec} [{jtz}] -> {cmd}")
            self.logger.info("Scheduler starting")
            self.scheduler.start()
        except Exception as e:
            self.logger.exception(f"Failed to start scheduler: {e}")
            # Fail fast so the SCM logs it
            raise

        # Wait until stop
        win32event.WaitForSingleObject(self.stop_event, win32event.INFINITE)
        self.logger.info("Python Runner service stopped")


if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(PythonRunnerService)
