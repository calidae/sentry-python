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
        from apache_beam.transforms.core import ParDo  # type: ignore

        old_init = ParDo.__init__

        def sentry_init_pardo(self, *args, **kwargs):
            old_init(self, *args, **kwargs)
            if not getattr(self, "_sentry_is_patched", False):
                # Need to patch both methods because older celery sometimes
                # short-circuits to task.run if it thinks it's safe.

                self.fn.process = _wrap_task_call(self.fn, self.fn.process, Hub.current.client)

                # `build_tracer` is apparently called for every task
                # invocation. Can't wrap every celery task for every invocation
                # or we will get infinitely nested wrapper functions.
                self._sentry_is_patched = True

            

        ParDo.__init__ = sentry_init_pardo


def _wrap_task_call(task, f, client):
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

def _capture_exception(task, exc_info, client):
    hub = Hub.current
    hub.bind_client(client)
    if hub.get_integration(BeamIntegration) is None:
        return
    if hasattr(task, "throws") and isinstance(exc_info[1], task.throws):
        return

    event, hint = event_from_exception(
        exc_info,
        client_options=hub.client.options,
        mechanism={"type": "beam", "handled": False},
    )

    if hub.capture_event(event, hint=hint) is None:
        print("NOOOOOO")


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
