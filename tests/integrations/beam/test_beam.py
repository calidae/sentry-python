import threading

import pytest
import dill
import inspect

# pytest.importorskip("apache-beam")

from sentry_sdk import Hub, configure_scope
from sentry_sdk.integrations.beam import BeamIntegration, _wrap_task_call, getfullargspec

from apache_beam.typehints.typehints import AnyTypeConstraint
from apache_beam.typehints.trivial_inference import instance_to_type
from apache_beam.typehints.decorators import getcallargs_forhints


def foo():
    return True


def bar(x, y):
    # print(x + y)
    return True


def baz(x, y=2):
    # print(x + y)
    return True


class A:
    def __init__(self, fn):
        self.r = "We are in A"
        self.fn = fn

    def process(self):
        return self.fn()


class B(A, object):
    def fa(self, x, element=False, another_element=False):
        if x or (element and not another_element):
            # print(self.r)
            return True
        print(1 / 0)
        return False

    def __init__(self):
        self.r = "We are in B"
        super(B, self).__init__(self.fa)


test_parent = A(foo)
test_child = B()

# print(type(test_parent))

def test_monkey_patch_getfullargspec():
    def check_fullargspec(f, *args, **kwargs):
        real_args = getfullargspec(f)

        if "self" in real_args.args:
            real_args.args.remove("self")

        fake_args = getfullargspec(_wrap_task_call(f))
        assert (
            real_args == fake_args
        ), "Assertion failed because {}, does not equal {} for {}".format(
            real_args, fake_args, f
        )
        assert f(*args, **kwargs)
        assert _wrap_task_call(f)(*args, **kwargs)

    check_fullargspec(foo)
    check_fullargspec(bar, 1, 5)
    check_fullargspec(baz, 1)
    check_fullargspec(test_parent.fn)
    check_fullargspec(test_child.fn, False, element=True)
    check_fullargspec(test_child.fn, True)
    print("Passed getfullargspec")


def test_monkey_patch_pickle():
    def check_pickling(f):
        f_temp = _wrap_task_call(f)
        # print(dill.detect.badobjects(f_temp, depth=1))
        # dill.detect.trace(True)
        # dill.pickles(f_temp)
        assert dill.pickles(f_temp), "{} is not pickling correctly!".format(f)

        # Pickle everything
        s1 = dill.dumps(f_temp)
        s2 = dill.loads(s1)
        s3 = dill.dumps(s2)
        # assert dill.loads(s3) == f_temp, "Function {} does not match with {}".format(dill.loads(s3), f_temp)

    check_pickling(foo)
    check_pickling(bar)
    check_pickling(baz)
    check_pickling(test_parent.fn)
    check_pickling(test_child.fn)
    print("Passed Pickling")


def test_monkey_patch_signature():
    def check_signature(f, *args, **kwargs):
        arg_types = [instance_to_type(v) for v in args]
        kwargs_types = {k: instance_to_type(v) for (k, v) in kwargs.items()}
        f_temp = _wrap_task_call(f)
        try:
            getcallargs_forhints(f_temp, *arg_types, **kwargs_types)
        except:
            print("Failed on {} with parameters {}, {}".format(f, args, kwargs))
            raise
        expected_signature = inspect.signature(f)
        test_signature = inspect.signature(_wrap_task_call(f))
        assert(expected_signature == test_signature), "Failed on {}, signature {} does not match {}".format(f, expected_signature, test_signature)

    check_signature(foo)
    check_signature(bar, 1, 5)
    check_signature(baz, 1)
    check_signature(test_parent.fn)
    check_signature(test_child.fn, False, element=True)
    check_signature(test_child.fn, True)
    print("Passed Signature")

test_monkey_patch_getfullargspec()
test_monkey_patch_pickle()
test_monkey_patch_signature()
