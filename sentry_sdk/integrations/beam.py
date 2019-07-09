from __future__ import absolute_import

import sys

from sentry_sdk.hub import Hub
from sentry_sdk.utils import capture_internal_exceptions, event_from_exception
from sentry_sdk.tracing import Span
from sentry_sdk._compat import reraise
from sentry_sdk.integrations import Integration
from sentry_sdk.integrations.logging import ignore_logger


class BeamIntegration(Integration):
    identifier = "beam"

    def __init__(self):
        pass

    @staticmethod
    def setup_once():
        # type: () -> None
        from beam.transforms.core import ParDo  # type: ignore

        old_init = ParDo.__init__

        def sentry_init_pardo(self, *args, **kwargs):
            old_init(self, *args, **kwargs)
            if not getattr(self, "_sentry_is_patched", False):
                # Need to patch both methods because older celery sometimes
                # short-circuits to task.run if it thinks it's safe.

                self.fn.process = _wrap_task_call(self.fn, self.fn.process)

                # `build_tracer` is apparently called for every task
                # invocation. Can't wrap every celery task for every invocation
                # or we will get infinitely nested wrapper functions.
                self._sentry_is_patched = True

            

        ParDo.__init__ = sentry_init_pardo


def _wrap_tracer(task, f):
    # Need to wrap tracer for pushing the scope before prerun is sent, and
    # popping it after postrun is sent.
    #
    # This is the reason we don't use signals for hooking in the first place.
    # Also because in Celery 3, signal dispatch returns early if one handler
    # crashes.
    def _inner(*args, **kwargs):
        hub = Hub.current
        if hub.get_integration(CeleryIntegration) is None:
            return f(*args, **kwargs)

        with hub.push_scope() as scope:
            scope._name = "celery"
            scope.clear_breadcrumbs()
            scope.add_event_processor(_make_event_processor(task, *args, **kwargs))

            span = Span.continue_from_headers(args[3].get("headers") or {})
            span.transaction = "unknown celery task"

            with capture_internal_exceptions():
                # Celery task objects are not a thing to be trusted. Even
                # something such as attribute access can fail.
                span.transaction = task.name

            with hub.span(span):
                return f(*args, **kwargs)

    return _inner


def _wrap_task_call(task, f):
    # Need to wrap task call because the exception is caught before we get to
    # see it. Also celery's reported stacktrace is untrustworthy.
    def _inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            exc_info = sys.exc_info()
            with capture_internal_exceptions():
                _capture_exception(task, exc_info)
            reraise(*exc_info)

    return _inner


def _make_event_processor(task, uuid, args, kwargs, request=None):
    def event_processor(event, hint):
        with capture_internal_exceptions():
            extra = event.setdefault("extra", {})
            extra["celery-job"] = {
                "task_name": task.name,
                "args": args,
                "kwargs": kwargs,
            }

        if "exc_info" in hint:
            with capture_internal_exceptions():
                if issubclass(hint["exc_info"][0], SoftTimeLimitExceeded):
                    event["fingerprint"] = [
                        "celery",
                        "SoftTimeLimitExceeded",
                        getattr(task, "name", task),
                    ]

        return event

    return event_processor


def _capture_exception(task, exc_info):
    hub = Hub.current

    if hub.get_integration(BeamIntegration) is None:
        return
    if hasattr(task, "throws") and isinstance(exc_info[1], task.throws):
        return

    event, hint = event_from_exception(
        exc_info,
        client_options=hub.client.options,
        mechanism={"type": "beam", "handled": False},
    )

    hub.capture_event(event, hint=hint)


def _patch_worker_exit():
    # Need to flush queue before worker shutdown because a crashing worker will
    # call os._exit
    from billiard.pool import Worker  # type: ignore

    old_workloop = Worker.workloop

    def sentry_workloop(*args, **kwargs):
        try:
            return old_workloop(*args, **kwargs)
        finally:
            with capture_internal_exceptions():
                hub = Hub.current
                if hub.get_integration(CeleryIntegration) is not None:
                    hub.flush()

    Worker.workloop = sentry_workloop
