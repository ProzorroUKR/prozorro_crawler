import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone

from aiohttp.abc import AbstractAccessLogger
from pythonjsonlogger import jsonlogger
from contextlib import contextmanager
from pythonjsonlogger.jsonlogger import merge_record_extra
from typing import Dict


LOG_CONTEXT: Dict[str, ContextVar] = {}


def update_log_context(**kwargs):
    tokens = {}
    for k, v in kwargs.items():
        if k not in LOG_CONTEXT:
            LOG_CONTEXT[k] = ContextVar(f"log_context_{k}")
        tokens[k] = LOG_CONTEXT[k].set(v)
    return tokens


def reset_log_context(tokens: dict, **kwargs):
    for k in kwargs:
        if k in LOG_CONTEXT and k in tokens:
            LOG_CONTEXT[k].reset(tokens[k])


@contextmanager
def log_context(**kwargs):
    tokens = update_log_context(**kwargs)
    try:
        yield
    finally:
        reset_log_context(tokens, **kwargs)


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        for field in self._required_fields:
            if field in self.rename_fields:
                log_record[self.rename_fields[field]] = record.__dict__.get(field)
            else:
                log_record[field] = record.__dict__.get(field)
        if not log_record['message'] and message_dict:
            log_record['message'] = message_dict
        else:
            log_record.update(message_dict)
        merge_record_extra(record, log_record, reserved=self._skip_fields)

        if self.timestamp:
            key = self.timestamp if type(
                self.timestamp) == str else 'timestamp'
            log_record[key] = datetime.fromtimestamp(record.created,
                                                     tz=timezone.utc)

        log_record['levelname'] = record.levelname
        log_record['name'] = record.name
        log_record['funcName'] = record.funcName

        # adding log context
        for k, var in LOG_CONTEXT.items():
            val = var.get("")
            if val:
                log_record[k] = val


def setup_logging():
    formatter = CustomJsonFormatter(json_ensure_ascii=False, timestamp=True)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logging.basicConfig(level=logging.DEBUG, handlers=[handler])

    # serve alternative logging for uncaught exceptions
    def exception_logging(exc_type, exc_value, exc_traceback):
        logging.exception(f'Exception {exc_type} raised', exc_info=exc_value)

    # override writing uncaught exceptions to stderr by using JSON logging
    sys.excepthook = exception_logging


# custom aiohttp access logger with request-id added
LOG_EXCLUDED = {
    "/api/ping",  # api ping
    "/api/metrics",  # metrics
}


class AccessLogger(AbstractAccessLogger):

    def log(self, request, response, time):
        remote = request.headers.get('X-Forwarded-For', request.remote)
        refer = request.headers.get('Referer', '-')
        user_agent = request.headers.get('User-Agent', '-')
        if request.path not in LOG_EXCLUDED:
            self.logger.info(f'{remote} '
                             f'"{request.method} {request.path} {response.status}'
                             f'{response.body_length} {refer} {user_agent} '
                             f'{time:.6f}s"')
