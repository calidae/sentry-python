from __future__ import absolute_import

import sys
import logging

from sentry_sdk.hub import Hub
from sentry_sdk.client import Client
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
        from apache_beam.transforms.core import ParDo, WindowInto  # type: ignore

        old_init = ParDo.__init__

        def sentry_init_pardo(self, *args, **kwargs):
            old_init(self, *args, **kwargs)

            if not getattr(self, "_sentry_is_patched", False):

                self.fn.process = _wrap_task_call(self.fn.process)
                self._sentry_is_patched = True

            

        ParDo.__init__ = sentry_init_pardo
        # old_window_init = WindowInto.__init__
        # def sentry_init_window(self, *args, **kwargs):
        #     if not getattr(self, "_sentry_is_patched", False):
        #         self.WindowIntoFn.process = _wrap_task_call(self.WindowIntoFn.process)
        #         self._sentry_is_patched = True
        #     old_window_init(self, *args, **kwargs)

        # WindowInto.__init__ = sentry_init_window
        ignore_logger("root")
        ignore_logger("bundle_processor.create")


def _wrap_task_call(f):
    client_dsn = Hub.current.client.dsn
    def _inner(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            exc_info = sys.exc_info()
            _capture_exception(exc_info, client_dsn)
            reraise(*exc_info)

    return _inner

def _capture_exception(exc_info, client_dsn):
    try:
        hub = Hub.current
        client = Client(dsn=client_dsn)
        hub.bind_client(client)
        ignore_logger("root")
        ignore_logger("bundle_processor.create")

        with capture_internal_exceptions():
            event, hint = event_from_exception(
                exc_info,
                client_options=client.options,
                mechanism={"type": "beam", "handled": False},
            )

            hub.capture_event(event, hint=hint)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e


