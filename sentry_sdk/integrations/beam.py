from __future__ import absolute_import

import sys
import logging
import re
import itertools

from sentry_sdk.hub import Hub
from sentry_sdk.client import Client
from sentry_sdk.utils import capture_internal_exceptions, event_from_exception
from sentry_sdk.tracing import Span
from sentry_sdk._compat import reraise
from sentry_sdk.integrations import Integration
from sentry_sdk.integrations.logging import ignore_logger

import inspect
import types
from inspect import getfullargspec
from functools import wraps


class BeamIntegration(Integration):
    identifier = "beam"

    def __init__(self):
        pass

    @staticmethod
    def setup_once():
        # type: () -> None
        from apache_beam.transforms.core import ParDo  # type: ignore

        old_init = ParDo.__init__

        def sentry_init_pardo(self, fn, *args, **kwargs):

            if not getattr(self, "_sentry_is_patched", False):

                fn.process = _wrap_task_call(fn.process)

                self._sentry_is_patched = True
            old_init(self, fn, *args, **kwargs)

        ParDo.__init__ = sentry_init_pardo

        ignore_logger("root")
        ignore_logger("bundle_processor.create")


def call_with_args(func):
    client = Hub.current.client
    evaldict = dict(
        _exep_=raiseException,
        _func_=func,
        _call_=_wrap_generator_call,
        Exception=Exception,
        client=client,
        types=types,
        wraps=wraps,
    )
    fun = FunctionMaker.create(func, evaldict)
    if hasattr(func, "__qualname__"):
        fun.__qualname__ = func.__qualname__
    return fun


def _wrap_generator_call(gen, client):
    while True:
        try:
            yield next(gen)
        except StopIteration:
            raise
        except:
            raiseException(client)


def raiseException(client):
    exc_info = sys.exc_info()
    _capture_exception(exc_info, client)
    reraise(*exc_info)

def _wrap_task_call(f):
    client = Hub.current.client
    _inner = Func(f, client) #call_with_args(f)
    _inner.__used__ = True
    if getattr(f, "__used__", False):
        return f
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



class Func(object):
    def __getattribute__(self, name):
        # if name == '__class__':
        #     # calling type(decorator()) will return <type 'function'>
        #     # this is used to trick the inspect module >:)
        #     print(super(Func, self).__getattribute__(name))
        #     print(type(self.__call__))
        #     return type(self.__call__)#types.FunctionType(self.__code__, self.__globals__)
        return super(Func, self).__getattribute__(name)

    def __init__(self, func, client):
        # self.__dict__ = func.__dict__
        # self.kwargs = self.argspec.kwargs
        self.func = func
        self.client = client
        self._init_func()

    def __call__(self, *args, **kwargs):
        try:
            # if "self" in argspec.args:
            #     args = args[1:]
            #     self.argspec.remove("self")
            self.args = args
            self.kwargs = kwargs
            gen = self.func(*args, **kwargs)
            # if not isinstance(gen, types.GeneratorType):
            #     return gen
            # gen = _wrap_generator_call(gen, self.client)
            return gen
        except Exception:
            # raiseException(self.client)
            print("Agh")

    def _init_func(self):
        func = self.func
        self.argspec = getfullargspec(func)
        # self.args = self.argspec.args
        self.__defaults__ = func.__defaults__
        self.__closure__ = func.__closure__
        self.__code__ = func.__code__
        self.__doc__ = func.__doc__
        self.__name__ = func.__name__
        self.__globals__ = func.__globals__
        self.__annotations__ = func.__annotations__
        self.__kwdefaults__ = func.__kwdefaults__
        self.__func__ = getattr(func, "__func__", None)
        self.__get__ = func.__get__

        if hasattr(func, "__qualname__"):
            self.__qualname__ = func.__qualname__
        self.args = None
        self.kwargs = None

    def __getstate__(self):
        return {"func": self.func, "client": self.client}

    def __setstate__(self, state):
        # print("STATE", state)
        self.func = state["func"]
        self.client = state["client"]
        self._init_func()


DEF = re.compile(r"\s*def\s*([_\w][_\w\d]*)\s*\(")


"""
Modified from micheles/decorator.py, https://github.com/micheles/decorator/blob/master/src/decorator.py.
"""
class FunctionMaker(object):

    _compile_count = itertools.count()

    def __init__(
        self, func=None, name=None, signature=None, defaults=None, doc=None, module=None
    ):
        self.shortsignature = signature
        if func:
            # func can be a class or a callable, but not an instance method
            self.name = func.__name__
            if self.name == "<lambda>":  # small hack for lambda functions
                self.name = "_lambda_"
            self.doc = func.__doc__
            self.module = func.__module__
            if inspect.ismethod(func) or inspect.isfunction(func):
                argspec = getfullargspec(func)
                self.annotations = getattr(func, "__annotations__", {})
                for a in (
                    "args",
                    "varargs",
                    "varkw",
                    "defaults",
                    "kwonlyargs",
                    "kwonlydefaults",
                ):
                    setattr(self, a, getattr(argspec, a))
                for i, arg in enumerate(self.args):
                    setattr(self, "arg%d" % i, arg)
                allargs = list(self.args)
                allshortargs = list(self.args)
                if self.varargs:
                    allargs.append("*" + self.varargs)
                    allshortargs.append("*" + self.varargs)
                elif self.kwonlyargs:
                    allargs.append("*")  # single star syntax
                for a in self.kwonlyargs:
                    allargs.append("%s=None" % a)
                    allshortargs.append("%s=%s" % (a, a))
                if self.varkw:
                    allargs.append("**" + self.varkw)
                    allshortargs.append("**" + self.varkw)
                if "self" in allargs:
                    allargs.remove("self")
                    allshortargs.remove("self")
                self.signature = ", ".join(allargs)
                self.shortsignature = ", ".join(allshortargs)
                self.dict = func.__dict__.copy()
        # func=None happens when decorating a caller
        if name:
            self.name = name
        if signature is not None:
            self.signature = signature
        if defaults:
            self.defaults = defaults
        if doc:
            self.doc = doc
        if module:
            self.module = module

        # check existence required attributes
        assert hasattr(self, "name")
        if not hasattr(self, "signature"):
            raise TypeError("You are decorating a non function: %s" % func)

    def update(self, func, **kw):
        "Update the signature of func with the data in self"
        func.__name__ = self.name
        func.__doc__ = getattr(self, "doc", None)
        func.__dict__ = getattr(self, "dict", {})
        func.__defaults__ = self.defaults
        func.__kwdefaults__ = self.kwonlydefaults or None
        func.__annotations__ = getattr(self, "annotations", None)
        try:
            frame = sys._getframe(3)
        except AttributeError:  # for IronPython and similar implementations
            callermodule = "?"
        else:
            callermodule = frame.f_globals.get("__name__", "?")
        func.__module__ = getattr(self, "module", callermodule)

        func.__dict__.update(kw)

    def make(self, src_templ, evaldict=None, addsource=False, **attrs):
        "Make a new function from a given template and update the signature"
        src = src_templ % vars(self)  # expand name and signature
        # raise Exception(src, evaldict)
        evaldict = evaldict or {}
        mo = DEF.search(src)
        if mo is None:
            raise SyntaxError("not a valid function template\n%s" % src)
        name = mo.group(1)  # extract the function name
        names = set(
            [name] + [arg.strip(" *") for arg in self.shortsignature.split(",")]
        )
        for n in names:
            if n in ("_func_", "_call_", "_exep_"):
                raise NameError("%s is overridden in\n%s" % (n, src))

        if not src.endswith("\n"):  # add a newline for old Pythons
            src += "\n"

        # Ensure each generated function has a unique filename for profilers
        # (such as cProfile) that depend on the tuple of (<filename>,
        # <definition line>, <function name>) being unique.
        filename = "<%s:decorator-gen-%d>" % (__file__, next(self._compile_count))
        localdict = {}
        try:
            # code = compile(src, filename, 'single')
            exec(src, evaldict, localdict)
        except Exception:
            print("Error in generated code:", file=sys.stderr)
            print(src, file=sys.stderr)
            raise
        func = localdict["_inner"]
 
        if addsource:
            attrs["__source__"] = src
        self.update(func, **attrs)
        return func

    @classmethod
    def create(
        cls,
        obj,
        evaldict,
        defaults=None,
        doc=None,
        module=None,
        addsource=True,
        **attrs
    ):
        """
        Create a function from the strings name, signature and body.
        evaldict is the evaluation dictionary. If addsource is true an
        attribute __source__ is added to the result. The attributes attrs
        are added, if any.
        """
        if isinstance(obj, str):  # "name(signature)"
            name, rest = obj.strip().split("(", 1)
            signature = rest[:-1]  # strip a right parens
            func = None
        else:  # a function
            name = None
            signature = None
            func = obj
        self = cls(func, name, signature, defaults, doc, module)
        body = """
@wraps(_func_)
def _inner(%(signature)s):
    try:
        gen = _func_(%(shortsignature)s)
        if not isinstance(gen, types.GeneratorType):
            return gen
        gen = _call_(gen, client)
        return gen
    except Exception:
        _exep_(client)
        raise
        """.strip()
        return self.make(body, evaldict, addsource, **attrs)
