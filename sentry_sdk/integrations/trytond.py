
import logging
import sentry_sdk
import sentry_sdk.integrations
import sentry_sdk.integrations.logging

from werkzeug.wrappers import Response
from trytond.exceptions import TrytonException as TrytondBaseException
from trytond.exceptions import UserError as TrytondUserError
from trytond.protocols.jsonrpc import JSONRequest
from trytond.wsgi import app


# TODO: trytond_worker intergation

class TrytondWSGIIntegration(sentry_sdk.integrations.Integration):
    identifier = "trytond_wsgi"

    def __init__(self):
        pass

    @staticmethod
    def setup_once():

        def error_handler(e):
            if isinstance(e, TrytondBaseException):
                return
            else:
                sentry_sdk.capture_exception(e)

        # Expected error handlers signature was changed
        # when the error_handler decorator was introduced
        # in Tryton-5.4
        if hasattr(app, 'error_handler'):
            @app.error_handler
            def _(app, request, e):
                error_handler(e)
        else:
            app.error_handlers.append(error_handler)


def rpc_error_page(app, request, e):
    if isinstance(e, TrytondBaseException):
        return
    else:
        event_id = sentry_sdk.last_event_id()
        data = TrytondUserError(
            str(event_id),
            'Event',
            str(e)
        )
        return app.make_response(
            request,
            data
        )


class TrytondSentryHandler(logging.NullHandler):

    def __init__(self, dsn, ignore=tuple()):
        super(TrytondSentryHandler, self).__init__()

        # There is no clean way to run a static configuration code block
        # when running the trytond-cron binary without crafting a whole
        # new binary that wraps it.
        # (See https://github.com/trytonus/trytond-sentry/blob/master/bin/trytond_sentry)
        # This is a workaround to provide a behaviour
        # similar to the old raven package

        sentry_sdk.init(dsn)

        # Also, there is no need to inherit
        # sentry_sdk.integrations.logging.EventHandler because
        # sentry_sdk.init will already install the default
        # LoggingIntegration that will already spy on any
        # logger.error call except for the following

        for logger in ignore:
            sentry_sdk.integrations.logging.ignore_logger(logger)

        # That is: there is no actual TrytondCronIntegration to be written
        # but this is needed so as to inject a sentry_sdk.init call
