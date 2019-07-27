from __future__ import absolute_import

import sys
import logging
import re
import itertools
import collections

from sentry_sdk.hub import Hub
from sentry_sdk.client import Client
from sentry_sdk.utils import capture_internal_exceptions, event_from_exception
from sentry_sdk.tracing import Span
from sentry_sdk._compat import reraise
from sentry_sdk.integrations import Integration
from sentry_sdk.integrations.logging import ignore_logger

import linecache

import inspect
import types

try:
    from inspect import getfullargspec
except:
    FullArgSpec = collections.namedtuple(
        'FullArgSpec', 'args varargs varkw defaults '
        'kwonlyargs kwonlydefaults annotations')
    def getfullargspec(f):
        "A quick and dirty replacement for getfullargspec for Python 2.X"
        return FullArgSpec._make(inspect.getargspec(f) + ([], None, {}))

class BeamIntegration(Integration):
    identifier = "beam"

    def __init__(self):
        pass

    @staticmethod
    def setup_once():
        # type: () -> None
        from apache_beam.transforms.core import ParDo, DoFn # type: ignore

        old_init = ParDo.__init__

        def sentry_init_pardo(self, fn, *args, **kwargs):

            if not getattr(self, "_sentry_is_patched", False):
                # og_start = fn.start_bundle
                # def new_start(*args, **kwargs):
                #     og_start(*args, **kwargs)

                #     fn.process =  _wrap_task_call(fn.process)
                
                # setattr(fn, "start_bundle", new_start)
                import __main__ as main
                filename = main.__file__
                fn.process =  _wrap_task_call(fn.process, filename)

                self._sentry_is_patched = True
            old_init(self, fn, *args, **kwargs)

        ParDo.__init__ = sentry_init_pardo
        # DoFn.__metaclass__ = MetaDoFn

        ignore_logger("root")
        ignore_logger("bundle_processor.create")


def _wrap_generator_call(gen, client):
    while True:
        try:
            yield next(gen)
        except StopIteration:
            raise
        except:
            raiseException(client)


def raiseException(client, passed_cache):
    print("Exception raised")
    print(passed_cache.keys())
    linecache.cache.update(passed_cache)
    exc_info = sys.exc_info()
    # exc_type, exc_value, tb = (exc_info)
    print("raiseException", exc_info)
    _capture_exception(exc_info, client)
    reraise(*exc_info)

def _wrap_task_call(f, filename):
    client = Hub.current.client
    # def _inner(*args, **kwargs):
    #     raise Exception("process")
    #     try:
    #         gen = f(*args, **kwargs)
    #         if not isinstance(gen, types.GeneratorType):
    #             return gen
    #         gen = _wrap_generator_call(gen, client)
    #         return gen
    #     except Exception:
    #         raiseException(client)

    _inner = Func.create(f, client, filename)

    return _inner


def _capture_exception(exc_info, client):
    hub = Hub.current
    if hub.client is None:
        hub.bind_client(client)
    integration = hub.get_integration(BeamIntegration)
    if integration:
        ignore_logger("root")
        ignore_logger("bundle_processor.create")
        with capture_internal_exceptions():
            event, hint = event_from_exception(
                exc_info,
                client_options=client.options,
                mechanism={"type": "beam", "handled": False},
            )

            hub.capture_event(event, hint=hint)



# class MetaDoFn:
#     def __new__(cls, *args, **kwargs):
#         raise Exception("Metalicious")
#         og_start = cls.start_bundle
#         def new_start(*args, **kwargs):
#             og_start(*args, **kwargs)
#             cls.process = _wrap_task_call(cls.process)
#         cls.start_bundle = new_start
#         return super().__new__(*args, **kwargs)


class Func:
    def __getattribute__(self, name):
        # if name == '__class__':
        # #     # calling type(decorator()) will return <type 'function'>
        # #     # this is used to trick the inspect module >:)
        # #     print(super(Func, self).__getattribute__(name))
        # #     print(type(self.__call__))
        # #     return type(self.__call__)#types.FunctionType(self.__code__, self.__globals__)
        #     return types.MethodType

        return super(Func, self).__getattribute__(name)

    def __init__(self, func, client, filename):
        # self.__dict__ = func.__dict__
        # self.kwargs = self.argspec.kwargs
        self.func = func
        self.client = client
        self.filename = filename
        if self.filename not in linecache.cache:
            linecache.getlines(self.filename)
        self.cache = linecache.cache.copy()
        self._init_func()

    def __call__(self, *args, **kwargs):
        # print("Call args", args, kwargs)
        try:
            # if "self" in self.argspec.args:
            #     args = args[1:]
            #     self.argspec.args.remove("self")
            linecache.cache.update(self.cache)
            self.args = args
            self.kwargs = kwargs
            gen = self.func(*args, **kwargs)
            if not isinstance(gen, types.GeneratorType):
                return gen
            gen = _wrap_generator_call(gen, self.client)
            return gen
        except Exception:
            raiseException(self.client, self.cache)
            #print("Agh")

    def _init_func(self):
        func = self.func
        self.__used__ = True
        # self.argspec = getfullargspec(func)
        # self.args = self.argspec.args
        self.__defaults__ = func.__defaults__
        self.__closure__ = func.__closure__
        self.__code__ = func.__code__
        self.__doc__ = func.__doc__
        self.__name__ = func.__name__
        self.__globals__ = func.__globals__
        self.__annotations__ = getattr(func, "__annotations__", None)
        self.__kwdefaults__ = getattr(func, "__kwdefaults__", None)
        self.__func__ = getattr(func, "__func__", None)
        self.__wrapped__ = func

        attributes = ["im_self", "func_code", "func_defaults", "func_closure", "func_dict", "func_doc", "func_globals", "func_name"]
        for attr in attributes:
            setattr(self, attr, getattr(func, attr, None))

        # self.__get__ = func.__get__

        if hasattr(func, "__qualname__"):
            self.__qualname__ = func.__qualname__
        self.args = None
        self.kwargs = None


    def __getstate__(self):
        return {"func": self.func, "client": self.client, "cache":self.cache}

    def __setstate__(self, state):
        # print("STATE", state)
        self.func = state["func"]
        self.client = state["client"]
        self.cache = state["cache"]
        self._init_func()

    def __get__(self, instance, owner):
        return partial(self, instance)

    @classmethod
    def create(self, func, client, filename):
        if getattr(func, "__used__", False):
            return func
        return self(func, client, filename)

