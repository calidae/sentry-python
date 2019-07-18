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
from inspect import getfullargspec


class BeamIntegration(Integration):
    identifier = "beam"

    def __init__(self):
        pass

    @staticmethod
    def setup_once():
        # type: () -> None
        from apache_beam.transforms.core import ParDo, WindowInto  # type: ignore
        from apache_beam.typehints.decorators import getfullargspec
        from apache_beam.transforms.core import get_function_arguments

        old_init = ParDo.__init__

        def sentry_init_pardo(self, *args, **kwargs):
            old_init(self, *args, **kwargs)

            if not getattr(self, "_sentry_is_patched", False):
                
                # get_function_arguments = _wrap_args_pec(getfullargspec, self.fn.process)
                # # getfullargspec = foo
                # original = self.fn.process
                self.fn.process = _wrap_task_call(self.fn, self.fn.process)
                # self.fn.process.__defaults__ = original.__defaults__
                # if (getfullargspec(original)[3]):
                #     raise Exception(get_function_arguments(self.fn, "process"), getfullargspec(original))
                # client_dsn = Hub.current.client.dsn
                # original = self.fn.process

                # setattr(self.fn, "process", call_with_args(self.fn, self.fn.process, _call_))


                # raise Exception(getfullargspec(self.fn.process))
                # if (getfullargspec(self.fn.process)[3]):
                # raise Exception(getfullargspec(original), getfullargspec(self.fn.process))
                self._sentry_is_patched = True
        # WindowInto.WindowIntoFn.process = _wrap_task_call(WindowInto.WindowIntoFn.process)
            

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

DEF = re.compile(r'\s*def\s*([_\w][_\w\d]*)\s*\(')

class FunctionMaker(object):

    _compile_count = itertools.count()


    def __init__(self, func=None, name=None, signature=None, defaults=None, doc=None, module=None):
        self.shortsignature = signature
        if func:
            # func can be a class or a callable, but not an instance method
            self.name = func.__name__
            if self.name == '<lambda>':  # small hack for lambda functions
                self.name = '_lambda_'
            self.doc = func.__doc__
            self.module = func.__module__
            if inspect.ismethod(func) or inspect.isfunction(func):
                argspec = getfullargspec(func)
                self.annotations = getattr(func, '__annotations__', {})
                for a in ('args', 'varargs', 'varkw', 'defaults', 'kwonlyargs',
                          'kwonlydefaults'):
                    setattr(self, a, getattr(argspec, a))
                for i, arg in enumerate(self.args):
                    setattr(self, 'arg%d' % i, arg)
                allargs = list(self.args)
                allshortargs = list(self.args)
                if self.varargs:
                    allargs.append('*' + self.varargs)
                    allshortargs.append('*' + self.varargs)
                elif self.kwonlyargs:
                    allargs.append('*')  # single star syntax
                for a in self.kwonlyargs:
                    allargs.append('%s=None' % a)
                    allshortargs.append('%s=%s' % (a, a))
                if self.varkw:
                    allargs.append('**' + self.varkw)
                    allshortargs.append('**' + self.varkw)
                if "self" in allargs:
                    allargs.remove("self")
                    allshortargs.remove("self")
                self.signature = ', '.join(allargs)
                self.shortsignature = ', '.join(allshortargs)
                self.dict = func.__dict__.copy()
        # func=None happens when decorating a caller
        # print(signature)
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
        assert hasattr(self, 'name')
        # print(hasattr(self, 'signature'))
        if not hasattr(self, 'signature'):
            raise TypeError('You are decorating a non function: %s' % func)

    def update(self, func, **kw):
        "Update the signature of func with the data in self"
        func.__name__ = self.name
        func.__doc__ = getattr(self, 'doc', None)
        func.__dict__ = getattr(self, 'dict', {})
        func.__defaults__ = self.defaults
        func.__kwdefaults__ = self.kwonlydefaults or None
        func.__annotations__ = getattr(self, 'annotations', None)
        try:
            frame = sys._getframe(3)
        except AttributeError:  # for IronPython and similar implementations
            callermodule = '?'
        else:
            callermodule = frame.f_globals.get('__name__', '?')
        func.__module__ = getattr(self, 'module', callermodule)
        func.__dict__.update(kw)

    def make(self, src_templ, evaldict=None, localdict=None, addsource=False, **attrs):
        "Make a new function from a given template and update the signature"
        src = src_templ % vars(self)  # expand name and signature
        # raise Exception(src, evaldict)
        evaldict = evaldict or {}
        mo = DEF.search(src)
        if mo is None:
            raise SyntaxError('not a valid function template\n%s' % src)
        name = mo.group(1)  # extract the function name
        names = set([name] + [arg.strip(' *') for arg in
                              self.shortsignature.split(',')])
        for n in names:
            if n in ('_func_', '_call_'):
                raise NameError('%s is overridden in\n%s' % (n, src))

        if not src.endswith('\n'):  # add a newline for old Pythons
            src += '\n'

        # Ensure each generated function has a unique filename for profilers
        # (such as cProfile) that depend on the tuple of (<filename>,
        # <definition line>, <function name>) being unique.
        filename = '<%s:decorator-gen-%d>' % (
            __file__, next(self._compile_count))
        try:
            code = compile(src, filename, 'single')
            exec(code, evaldict, localdict)
            # raise Exception(evaldict)
        except Exception:
            print('Error in generated code:', file=sys.stderr)
            print(src, file=sys.stderr)
            raise
        func = localdict["test"]
        # raise Exception(getfullargspec(func))
        if addsource:
            attrs['__source__'] = src
        self.update(func, **attrs)
        return func

    @classmethod
    def create(cls, obj, evaldict, localdict, defaults=None,
               doc=None, module=None, addsource=True, **attrs):
        """
        Create a function from the strings name, signature and body.
        evaldict is the evaluation dictionary. If addsource is true an
        attribute __source__ is added to the result. The attributes attrs
        are added, if any.
        """
        if isinstance(obj, str):  # "name(signature)"
            name, rest = obj.strip().split('(', 1)
            signature = rest[:-1]  # strip a right parens
            func = None
        else:  # a function
            name = None
            signature = None
            func = obj
        # raise Exception(getfullargspec(func))
        self = cls(func, name, signature, defaults, doc, module)
        # ibody = '\n'.join('    ' + line for line in body.splitlines())
        body = '''
def test(%(signature)s):
    try:
        return _func_(%(shortsignature)s)
    except:
        _call_()
        raise
        '''.strip()
        return self.make(body, evaldict, localdict, addsource, **attrs)

def call_with_args(self, func, exep):
    localdict = dict(self=self)
    evaldict = dict(_call_=exep, _func_=func, print=print)
    fun = FunctionMaker.create(
            func, evaldict, localdict, __wrapped__=func)
    if hasattr(func, '__qualname__'):
        fun.__qualname__ = func.__qualname__
    return fun

def _call_():
    client_dsn = Hub.current.client.dsn
    exc_info = sys.exc_info()
    _capture_exception(exc_info, client_dsn)
    reraise(*exc_info)

def _wrap_task_call(self, f):
    # client_dsn = Hub.current.client.dsn
    # exc_info = sys.exc_info()
    # _capture_exception(exc_info, client_dsn)
    # reraise(*exc_info)
    client_dsn = Hub.current.client.dsn
    # from apache_beam.transforms.core import DoFn, _DoFnParam
    # from apache_beam.typehints.decorators import getfullargspec
    # import inspect
    # from functools import wraps
    # # import wrapt
    # import decorator
    # # gfa = getfullargspec(f)
    # # func_args = gfa[0]
    # # default_args = gfa[3]
    # # print(gfa)
    # # if default_args:
    # #     for arg_idx in range(len(default_args)):
    # #         count = len(func_args)-len(default_args)+arg_idx
    # #         kwargs[func_args[count]] = default_args[arg_idx]



    # # @decor
    def _inner(*args, **kwargs):
        # _inner.__dict__.update(**kwargs)
        try:
            return f(*args, **kwargs)
        except Exception:
            exc_info = sys.exc_info()
            _capture_exception(exc_info, client_dsn)
            reraise(*exc_info)
    if getfullargspec(f)[3]:
        _inner = call_with_args(self, f, _call_)
    # # args = getfullargspec(f).args
    # # _inner.__getstate__ = f.__getstate__
    # # _inner.__setstate__ = f.__setstate__
    # # _inner.args = f.args
    # # raise Exception(f.__closure__[1])
    # # _inner.args = args
    # # # _inner.__kwdefaults__ = f.__kwdefaults__
    # if getfullargspec(f)[3]:
    #     # _inner = decor(f)
    # #     f_temp = decorator(f)
    # #     def something(*args, **kwargs):
    # #         return f_temp(*args, **kwargs)
    # #     return something
    # #     _inner.__call__ = f.__call__
    # #     # _inner.__closure__ = f.__closure__
    # #     # _inner.__code__ = f.__code__
    # #     _inner.__doc__ = f.__doc__
    # #     _inner.__name__ = f.__name__
    # #     _inner.__defaults__ = f.__defaults__
    # #     # _inner.__globals__ = f.__globals__
    # #     _inner.__get__ = f.__get__
    # #     _inner.__defaults__ = f.__defaults__
    # #     # inspect.full
    # #     # _inner.__code__ = f.__code__
    # #     # raise(Exception(dir(f)))
    # #     # _inner.__closure__[1].cell_contents.args = args
    #     _inner = decor(f)
    #     setattr(cls, "f", decor(getattr(cls, "f").im_func))
    return _inner

def _wrap_args_pec(original_args, f):
    def _inner(*args, **kwargs):
        return original_args(f)
    return _inner

def _capture_exception(exc_info, client_dsn):
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



