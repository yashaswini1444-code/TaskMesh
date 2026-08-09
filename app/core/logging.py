"""Minimal structured-enough application logging.

Deliberately not a JSON/ELK/OpenTelemetry setup — see docs/architecture.md
for what this does and does not provide. Every message already embeds its
own key=value context (task_id, attempt_number, etc.), which is enough to
grep/filter without a log-shipping pipeline this project doesn't have.

Configures only the "taskmesh" logger namespace with its own handler,
independent of whatever the host process (uvicorn, Celery) does with the
root logger — calling logging.basicConfig() here would risk clobbering
uvicorn's own access/error log formatting, which is depended on elsewhere.
"""

from __future__ import annotations

import logging

_configured = False


def configure_logging(level: int = logging.INFO) -> None:
    global _configured
    if _configured:
        return

    app_logger = logging.getLogger("taskmesh")
    app_logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s"))
    app_logger.addHandler(handler)
    app_logger.propagate = False  # avoid duplicate lines if the root logger also has a handler

    _configured = True
