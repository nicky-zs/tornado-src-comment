# vim: fileencoding=utf-8

from __future__ import absolute_import, division, with_statement

import functools
import operator
import sys
import types

from tornado.stack_context import ExceptionStackContext


class KeyReuseError(Exception)       : pass
class UnknownKeyError(Exception)     : pass
class LeakedCallbackError(Exception) : pass
class BadYieldError(Exception)       : pass


def engine(func):
    """ 使用生成器写异步callback代码的函数的装饰器。
    所有yield本模块的对象（Task等）的生成器都要使用此装饰器。此装饰器只能应用于异步的代码，比如异步的get/post。
    （为了正确的异常处理，tornado.gen.engine装饰器要离生成器更近一些。）
    在多数情况下，对那些没有callback参数的函数使用此装饰器是没有意义的。 """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        runner = None # 生成器的执行依赖于一个Runner。每一个engine在被调用的时候都会转换成一个Runner然后去执行。
        def handle_exception(typ, value, tb):
            # if the function throws an exception before its first "yield" (or is not a generator at all), the Runner won't exist yet.
            # However, in that case we haven't reached anything asynchronous yet, so we can just let the exception propagate.
            if runner is not None:
                return runner.handle_exception(typ, value, tb)
            return False
        with ExceptionStackContext(handle_exception) as deactivate:
            gen = func(*args, **kwargs) # 调用func，得到gen。无论func是生成器函数，还是普通异步函数，这里都不会阻塞。
            # 如果func是普通同步函数，则这里已经完成所有的工作了，只要func不返回非None的值，则该engine正常返回。
            if isinstance(gen, types.GeneratorType): # 如果gen是一个生成器，则构建一个Runner对象去运行它
                runner = Runner(gen, deactivate)
                runner.run() # 只要func中yield的Task包装的是异步方法，则该run()方法不会被阻塞
                return
            # 如果gen不是生成器（即func中没有yield），则它必须是None（即不func不能返回任何值），否则报错
            # 且此时不用再做任何事情，因为func的调用已经结束了。
            assert gen is None, gen
            deactivate()
    return wrapper


class YieldPoint(object):
    """ 可以在tornado.gen.engine装饰的生成器中yield出去的对象的基类。其支持的操作有：启动、判断是否就绪、取出结果。 """

    def start(self, runner):
        """ 当生成器yield之后，该方法会被Runner调用。这是该对象第一个被调用的方法。 """
        raise NotImplementedError()

    def is_ready(self):
        """ 由Runner调用，以判断是否重新开始生成器。返回一个布尔值，可多次调用。 """
        raise NotImplementedError()

    def get_result(self):
        """ 返回yield表达式的结果的值。该方法仅在is_ready方法返回True时被调用一次。 """
        raise NotImplementedError()


class Callback(YieldPoint):
    """Returns a callable object that will allow a matching `Wait` to proceed.

    The key may be any value suitable for use as a dictionary key, and is
    used to match ``Callbacks`` to their corresponding ``Waits``.  The key
    must be unique among outstanding callbacks within a single run of the
    generator function, but may be reused across different runs of the same
    function (so constants generally work fine).

    The callback may be called with zero or one arguments; if an argument
    is given it will be returned by `Wait`.
    """
    def __init__(self, key):
        self.key = key

    def start(self, runner):
        self.runner = runner
        runner.register_callback(self.key)

    def is_ready(self):
        return True

    def get_result(self):
        return self.runner.result_callback(self.key)


class Wait(YieldPoint):
    """Returns the argument passed to the result of a previous `Callback`."""
    def __init__(self, key):
        self.key = key

    def start(self, runner):
        self.runner = runner

    def is_ready(self):
        return self.runner.is_ready(self.key)

    def get_result(self):
        return self.runner.pop_result(self.key)


class WaitAll(YieldPoint):
    """Returns the results of multiple previous `Callbacks`.

    The argument is a sequence of `Callback` keys, and the result is
    a list of results in the same order.

    `WaitAll` is equivalent to yielding a list of `Wait` objects.
    """
    def __init__(self, keys):
        self.keys = keys

    def start(self, runner):
        self.runner = runner

    def is_ready(self):
        return all(self.runner.is_ready(key) for key in self.keys)

    def get_result(self):
        return [self.runner.pop_result(key) for key in self.keys]


class Task(YieldPoint):
    """ 运行单个的异步操作。 """
    def __init__(self, func, *args, **kwargs):
        assert "callback" not in kwargs # func是一个接受callback的异步函数，但是参数不要传callback进来，func的callback会被统一设置
        self.args = args
        self.kwargs = kwargs
        self.func = func

    def start(self, runner):
        self.runner = runner
        self.key = object() # 对于不同的Task，runner可能是同一个，但是key会不一样
        runner.register_callback(self.key) # 每一个Task对象会在runner中注册一个key
        self.kwargs["callback"] = runner.result_callback(self.key) # callback被统一设置为runner的result_callback(self.key)
        self.func(*self.args, **self.kwargs) # 以runner的result_callback作为callback来调用包装的func。
        # 这里的func通常应该是异步函数，调用后会马上返回。于是self.is_ready()通常为False，这会使得runner的run方法退出。
        # 等操作完成后会自动去调用self.kwargs["callback"]函数。而这个回调函数会去启动runner的下一轮run()操作。

    def is_ready(self):
        return self.runner.is_ready(self.key)

    def get_result(self):
        return self.runner.pop_result(self.key)


class Multi(YieldPoint):
    """Runs multiple asynchronous operations in parallel.

    Takes a list of ``Tasks`` or other ``YieldPoints`` and returns a list of
    their responses.  It is not necessary to call `Multi` explicitly,
    since the engine will do so automatically when the generator yields
    a list of ``YieldPoints``.
    """
    def __init__(self, children):
        assert all(isinstance(i, YieldPoint) for i in children)
        self.children = children

    def start(self, runner):
        for i in self.children:
            i.start(runner)

    def is_ready(self):
        return all(i.is_ready() for i in self.children)

    def get_result(self):
        return [i.get_result() for i in self.children]


class _NullYieldPoint(YieldPoint):
    """ 空任务。任何时候都是ready状态，且结果都为None。 """
    def start(self, runner):
        pass
    def is_ready(self):
        return True
    def get_result(self):
        return None


class Runner(object):
    """ tornado.gen.engine的内部实现。维护未决的callback和他们结果的信息。 """
    # 每一个由engine修饰的生成器在被调用时都会被转化成一个Runner对象，并调用run()方法。

    def __init__(self, gen, deactivate_stack_context):
        self.gen = gen # 该Runner要去运行的生成器
        self.deactivate_stack_context = deactivate_stack_context
        self.yield_point = _NullYieldPoint() # 初始给定一个空任务
        self.pending_callbacks = set() # 回调key的集合
        self.results = {} # 结果字典
        self.running = False # 某一个yield表达式在运行
        self.finished = False # 整个生成器运行完毕
        self.exc_info = None
        self.had_exception = False

    def register_callback(self, key):
        if key in self.pending_callbacks:
            raise KeyReuseError("key %r is already pending" % key)
        self.pending_callbacks.add(key)

    def is_ready(self, key):
        if key not in self.pending_callbacks:
            raise UnknownKeyError("key %r is not pending" % key)
        return key in self.results # 通过key是否在结果字典中来判断

    def set_result(self, key, result): # Task包装的异步函数真正的callback
        self.results[key] = result # 将结果以key设置到runner的结果字典中
        self.run() # 设置完结果后让runner继续运行

    def pop_result(self, key):
        self.pending_callbacks.remove(key)
        return self.results.pop(key)

    def run(self):
        """ 启动或者继续运行生成器，运行直到到达了一个未准备好的YieldPoint。 """
        if self.running or self.finished:
            return
        try:
            self.running = True
            while True: # 第一轮循环会以self.yield_point=_NullYieldPoint()空跑一轮，直接把None发送给生成器以得到第一个YieldPoint
                if self.exc_info is None:
                    try:
                        if not self.yield_point.is_ready(): # 遇到了一个未准备好的YieldPoint，直接退出
                            return
                        # 如果YieldPoint已经ready则取出结果，该结果是生成器yield出来的，应该send回生成器：
                        # def gen_func():
                        #     result = yield gen.Task(get_user_by_id, id=123)
                        # 从而使生成器可以拿到yield表达式的返回值，继续运行
                        next = self.yield_point.get_result()
                    except Exception:
                        self.exc_info = sys.exc_info() # 如果有异常就记录在self.exc_info中
                try:
                    if self.exc_info is not None: # 在调用YieldPoint时发生了异常
                        self.had_exception = True
                        exc_info = self.exc_info
                        self.exc_info = None
                        yielded = self.gen.throw(*exc_info) # 把异常抛回给生成器，并拿到生成器yield出来的结果
                    else:
                        yielded = self.gen.send(next) # 如果没异常，则把结果send回生成器，并拿到生成器yield出来的结果
                except StopIteration: # 生成器执行结束
                    self.finished = True
                    if self.pending_callbacks and not self.had_exception:
                        # If we ran cleanly without waiting on all callbacks raise an error (really more of a warning).
                        # If we had an exception then some callbacks may have been orphaned, so skip the check in that case.
                        raise LeakedCallbackError("finished without waiting for callbacks %r" % self.pending_callbacks)
                    self.deactivate_stack_context()
                    self.deactivate_stack_context = None
                    return
                except Exception:
                    self.finished = True
                    raise
                if isinstance(yielded, list): # 如果生成器yield了一个list出来，则用Multi包装一下
                    yielded = Multi(yielded)
                if isinstance(yielded, YieldPoint):
                    self.yield_point = yielded # 把yielded赋给self.yield_point以供下一次run()来调用
                    try:
                        self.yield_point.start(self) # 继续使用该runner来启动yield出来的YieldPoint
                        # 通常，这个start操作会调用异步方法，马上返回。然而，在该异步函数的callback被调用之前，该yield_point会
                        # 一直处于未完成状态。这样，在下一轮while循环的一开始，循环便会因is_ready()为False而导致run()返回。
                    except Exception:
                        self.exc_info = sys.exc_info()
                else: # 生成器里只能yield所有YieldPoint子类的对象
                    self.exc_info = (BadYieldError("yielded unknown object %r" % yielded),)
        finally:
            self.running = False

    def result_callback(self, key): # 调用此函数以生成一个与key绑定的闭包回调
        def inner(*args, **kwargs): # 该函数只是简单地包装了一下结果，然后调用set_result
            if kwargs or len(args) > 1: # 如果结果不止一个，则包装成Arguments对象
                result = Arguments(args, kwargs)
            elif args:
                result = args[0]
            else:
                result = None
            self.set_result(key, result) # 将结果按key存放在self.results集合中，并重新启动该runner
        return inner

    def handle_exception(self, typ, value, tb):
        if not self.running and not self.finished:
            self.exc_info = (typ, value, tb)
            self.run()
            return True
        else:
            return False


# in python 2.6+ this could be a collections.namedtuple
class Arguments(tuple): # 把args和kwargs包装到一个tuple中
    __slots__ = ()

    def __new__(cls, args, kwargs):
        return tuple.__new__(cls, (args, kwargs))

    args = property(operator.itemgetter(0))
    kwargs = property(operator.itemgetter(1))

