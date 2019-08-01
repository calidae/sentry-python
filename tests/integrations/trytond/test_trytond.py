
import json
import pytest

pytest.importorskip("trytond")

from trytond.exceptions import UserError as TrytondUserError
from trytond.exceptions import UserWarning as TrytondUserWarning
from trytond.exceptions import LoginException
from trytond.wsgi import app as trytond_app

from werkzeug.test import Client
from sentry_sdk import capture_message
from sentry_sdk.integrations.trytond import TrytondWSGIIntegration
from sentry_sdk.integrations.trytond import rpc_error_page


@pytest.fixture(scope="function")
def app(sentry_init):
    yield trytond_app


@pytest.fixture
def get_client(app):
    def inner():
        return Client(app)
    return inner


@pytest.mark.parametrize("exception", [
    Exception('foo'),
    type('FooException', (Exception,), {})('bar'),
])
def test_exceptions_captured(sentry_init, app, capture_exceptions, get_client, exception):
    sentry_init(integrations=[TrytondWSGIIntegration()])
    exceptions = capture_exceptions()

    @app.route('/exception')
    def _(request):
        raise exception

    client = get_client()
    response = client.get("/exception")

    (e,) = exceptions
    assert e is exception


@pytest.mark.parametrize("exception", [
    TrytondUserError('title'),
    TrytondUserWarning('title', 'details'),
    LoginException('title', 'details'),
])
def test_trytonderrors_not_captured(sentry_init, app, capture_exceptions, get_client, exception):
    sentry_init(integrations=[TrytondWSGIIntegration()])
    exceptions = capture_exceptions()

    @app.route('/usererror')
    def _(request):
        raise exception

    client = get_client()
    response = client.get("/usererror")

    assert not exceptions


def test_rpc_error_page(sentry_init, app, capture_events, get_client):
    sentry_init(integrations=[TrytondWSGIIntegration()])
    events = capture_events()

    @app.route('/rpcerror', methods=['POST'])
    def _(request):
        raise Exception('foo')

    app.append_err_handler(rpc_error_page)

    client = get_client()
    # This would look like a natural Tryton RPC call
    _ids = [1234]
    _values = ["values"]
    _context = dict(
        client='12345678-9abc-def0-1234-56789abc',
        groups=[1],
        language='ca',
        language_direction='ltr',
    )
    response = client.post(
        "/rpcerror",
        content_type="application/json",
        data=json.dumps(dict(
            id=42,
            method='class.method',
            params=[_ids, _values, _context],
        ))
    )

    (event,) = events
    (content, status, headers) = response
    assert status == '200 OK'
    assert headers.get('Content-Type') == 'application/json'
    data = json.loads(next(content))
    assert data == dict(
        error=['UserError', [event['event_id'], 'foo']],
        id=None  # FIXME: 42
    )
