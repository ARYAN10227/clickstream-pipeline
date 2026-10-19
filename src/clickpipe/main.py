"""Consumer entrypoint: `python -m clickpipe.main` or the `clickpipe` console script."""

import logging
import signal
import sys
import threading
from datetime import UTC, datetime
from types import FrameType

import orjson

from clickpipe.config import get_settings
from clickpipe.consumer import ClickConsumer
from clickpipe.dlq import DlqProducer
from clickpipe.sink import ClickHouseSink

log = logging.getLogger("clickpipe")


class JsonFormatter(logging.Formatter):
    """Format each log record as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return orjson.dumps(payload).decode()


def configure_logging(level: int = logging.INFO) -> None:
    """Send JSON log lines to stdout."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)


def main() -> int:
    """Run the consumer until SIGTERM/SIGINT; return the process exit code."""
    configure_logging()
    settings = get_settings()
    stop = threading.Event()

    def request_stop(signum: int, _frame: FrameType | None) -> None:
        log.info("received %s, finishing current batch", signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    try:
        sink = ClickHouseSink(settings)
        dlq = DlqProducer(settings)
        consumer = ClickConsumer(settings, sink, dlq)
    except Exception as exc:
        log.error(
            "consumer failed to start (%s: %s); exiting with code 1 so it is restarted",
            type(exc).__name__,
            exc,
        )
        return 1
    log.info(
        "consumer started: topic=%s dlq=%s group=%s bootstrap=%s clickhouse=%s:%d batch=%d/%dms",
        settings.clicks_topic,
        settings.dlq_topic,
        settings.consumer_group,
        settings.kafka_bootstrap,
        settings.clickhouse_host,
        settings.clickhouse_port,
        settings.batch_max_size,
        settings.batch_max_wait_ms,
    )
    exit_code = 0
    try:
        consumer.run(stop)
    except Exception as exc:
        # Unrecoverable (e.g. inserts still failing after all retries). The failed batch was
        # NOT committed, and auto-commit is off, so closing below commits nothing: after the
        # restart (compose restart policy) the same messages are delivered again.
        log.exception(
            "consumer stopped on unrecoverable %s; batch not committed, exiting with code 1",
            type(exc).__name__,
        )
        exit_code = 1
    finally:
        consumer.close()
        try:
            dlq.close()
        except Exception:
            log.exception("DLQ producer did not flush cleanly on shutdown")
        sink.close()
        log.info("consumer closed (exit code %d)", exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
