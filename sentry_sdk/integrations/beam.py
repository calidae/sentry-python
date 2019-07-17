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

# import types

# class decorator(object):
#     def __getattribute__(self, name):
#         if name == '__class__':
#             # calling type(decorator()) will return <type 'function'>
#             # this is used to trick the inspect module >:)
#             return types.FunctionType
#         return super(decorator, self).__getattribute__(name)

#     def __init__(self, fn):
#         # let's pretend for just a second that this class
#         # is actually a function. Explicity copying the attributes
#         # allows for stacked decorators.
#         self.__call__ = fn.__call__
#         self.__closure__ = fn.__closure__
#         self.__code__ = fn.__code__
#         self.__doc__ = fn.__doc__
#         self.__name__ = fn.__name__
#         self.__defaults__ = fn.__defaults__
#         self.__globals__ = fn.__globals__
#         self.__get__ = fn.__get__
#         # self.__dict__ = fn.__dict__
#         # raise Exception(dir(fn))
#         # self.im_class = fn.im_class
#         # self.im_func = fn.im_func
#         # self.im_self = fn.im_self

#         # self.original_attr = fn.__getattribute__
#         # self.__getattribute__ = self.getattribute
#         self.func_defaults = fn.func_defaults
#         # self.func_closure = fn.func_closure
#         self.func_code = fn.func_code
#         # self.func_dict = fn.func_dict
#         # self.func_doc = fn.func_doc
#         # self.func_globals = fn.func_globals
#         # self.func_name = fn.func_name

#         # any attributes that need to be added should be added
#         # *after* converting the class to a function
#         self.args = None
#         self.kwargs = None
#         self.result = None
#         self.function = fn

#     def __call__(self, *args, **kwargs):
#         self.args = args
#         self.kwargs = kwargs
#         try:
#             self.result = self.function(*args, **kwargs)
#         except:
#             exc_info = sys.exc_info()
#             _capture_exception(exc_info, client_dsn)
#             reraise(*exc_info)

#         return self.result
#     def __getstate__(self):
#         d = {"code": self.func_code}
#         return d.update(self.__dict__)
#     def __setstate_(self, data):
#         self.__dict__.update(data)

def _wrap_task_call(f):
    client_dsn = Hub.current.client.dsn
    # from apache_beam.transforms.core import DoFn, _DoFnParam
    from apache_beam.typehints.decorators import getfullargspec
    import inspect
    from functools import wraps
    # import wrapt
    import decorator
    # gfa = getfullargspec(f)
    # func_args = gfa[0]
    # default_args = gfa[3]
    # print(gfa)
    # if default_args:
    #     for arg_idx in range(len(default_args)):
    #         count = len(func_args)-len(default_args)+arg_idx
    #         kwargs[func_args[count]] = default_args[arg_idx]

    @decorator.decorator
    def decor(f, *args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            exc_info = sys.exc_info()
            _capture_exception(exc_info, client_dsn)
            reraise(*exc_info)

    # @decor
    def _inner(*args, **kwargs):
        # _inner.__dict__.update(**kwargs)
        try:
            return f(*args, **kwargs)
        except Exception:
            exc_info = sys.exc_info()
            _capture_exception(exc_info, client_dsn)
            reraise(*exc_info)
    # args = getfullargspec(f).args
    # _inner.__getstate__ = f.__getstate__
    # _inner.__setstate__ = f.__setstate__
    # _inner.args = f.args
    # raise Exception(f.__closure__[1])
    # _inner.args = args
    # # _inner.__kwdefaults__ = f.__kwdefaults__
    if getfullargspec(f)[3]:
        # _inner = decor(f)
    #     f_temp = decorator(f)
    #     def something(*args, **kwargs):
    #         return f_temp(*args, **kwargs)
    #     return something
    #     _inner.__call__ = f.__call__
    #     # _inner.__closure__ = f.__closure__
    #     # _inner.__code__ = f.__code__
    #     _inner.__doc__ = f.__doc__
    #     _inner.__name__ = f.__name__
    #     _inner.__defaults__ = f.__defaults__
    #     # _inner.__globals__ = f.__globals__
    #     _inner.__get__ = f.__get__
    #     _inner.__defaults__ = f.__defaults__
    #     # inspect.full
    #     # _inner.__code__ = f.__code__
    #     # raise(Exception(dir(f)))
    #     # _inner.__closure__[1].cell_contents.args = args
        _inner = decor(f)
        print("OSMAR", getfullargspec(_inner).defaults)
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



