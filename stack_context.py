# vim: fileencoding=utf-8

""" StackContext允许程序维护类threadlocal的状态，可以随着执行上下文一起转移。
大多数程序都不用直接操作StackContext。有必要的情况有：
* If you're writing an asynchronous library that doesn't rely on a stack_context-aware library like `tornado.ioloop`
  or `tornado.iostream` (for example, if you're writing a thread pool), use `stack_context.wrap()` before any asynchronous
  operations to capture the stack context from where the operation was started.
* If you're writing an asynchronous library that has some shared resources (such as a connection pool), create those shared resources
  within a ``with stack_context.NullContext():`` block.  This will prevent ``StackContexts`` from leaking from one request to another.
* If you want to write something like an exception handler that will persist across asynchronous calls, create a new `StackContext` (or
  `ExceptionStackContext`), and make your asynchronous calls in a ``with`` block that references your `StackContext`.
"""

from __future__ import absolute_import, division, with_statement

import contextlib
import functools
import itertools
import operator
import sys
import threading

from tornado.util import raise_exc_info


class _State(threading.local):
    def __init__(self):
        self.contexts = ()
_state = _State()


class StackContext(object):
    """ 把给定的上下文建立成一个StackContext对象以用于转移。
    注意，参数是一个callable，它返回一个contextmanager，而不是上下文本身。即对于一个不可转移的contextmanager：
      with my_context():
    StackContext以该函数本身作为参数，而不是结果：
      with StackContext(my_context):

    The result of ``with StackContext() as cb:`` is a deactivation callback.
    Run this callback when the StackContext is no longer needed to ensure that it is not propagated any further
    (note that deactivating a context does not affect any instances of that context that are currently pending).
    This is an advanced feature and not necessary in most applications. """ 
    def __init__(self, context_factory, _active_cell=None):
        self.context_factory = context_factory # 调用该函数能得到一个contextmanager
        self.active_cell = _active_cell or [True]

    # Note that some of this code is duplicated in ExceptionStackContext below.
    # ExceptionStackContext is more common and doesn't need the full generality of this class.
    def __enter__(self):
        self.old_contexts = _state.contexts # _state.contexts是(class, arg, active_cell)三元组
        # 进入StackContext时，self.old_contexts会保留旧的上下文，当前上下文会追加一个三元组
        _state.contexts = (self.old_contexts + ((StackContext, self.context_factory, self.active_cell),))
        try:
            self.context = self.context_factory()
            self.context.__enter__()
        except Exception:
            _state.contexts = self.old_contexts
            raise
        return lambda: operator.setitem(self.active_cell, 0, False) # 该lambda表达式即deactivation

    def __exit__(self, type, value, traceback):
        try:
            return self.context.__exit__(type, value, traceback)
        finally:
            _state.contexts = self.old_contexts


class ExceptionStackContext(object):
    """ StackContext用于异常处理的子类。
    提供的异常处理函数会在context中捕获到异常时被调用，语义类似try/finally，用于记录log，关闭socket等cleanup操作。
    The exc_info triple (type, value, traceback) will be passed to the exception_handler function.
    If the exception handler returns true, the exception will be consumed and will not be propagated to other exception handlers. """ 
    def __init__(self, exception_handler, _active_cell=None):
        self.exception_handler = exception_handler
        self.active_cell = _active_cell or [True]

    def __enter__(self):
        self.old_contexts = _state.contexts
        # 进入该context时，会在_state.contexts的基础上加入exception_handler的context
        _state.contexts = (self.old_contexts + ((ExceptionStackContext, self.exception_handler, self.active_cell), ))
        return lambda: operator.setitem(self.active_cell, 0, False) # 即deactivation对象

    def __exit__(self, type, value, traceback):
        try:
            if type is not None: # 如果在退出时有异常，则调用exception_handler来处理这异常
                return self.exception_handler(type, value, traceback)
        finally:
            _state.contexts = self.old_contexts
            self.old_contexts = None


class NullContext(object):
    """ 重置StackContext。
    Useful when creating a shared resource on demand (e.g. an AsyncHTTPClient)
    where the stack that caused the creating is not relevant to future operations. """
    def __enter__(self):
        self.old_contexts = _state.contexts
        _state.contexts = () # 使callback运行时_state.contexts为空，从而完全从包装时的contexts来生成new_contexts

    def __exit__(self, type, value, traceback):
        _state.contexts = self.old_contexts


class _StackContextWrapper(functools.partial):
    pass


def wrap(fn):
    """ 返回一个可调用的对象，该对象在执行后会恢复当前的StackContext。
    当保存一个回调以便稍后在不同的上下文中执行时，使用该修饰器。 """
    if fn is None or fn.__class__ is _StackContextWrapper:
        return fn

    #@functools.wraps(fn) # functools.wraps不能在functools.partial对象上工作
    def wrapped(*args, **kwargs):
        # callback和contexts是由_StackContextWrapper即functools.partial带入的包装fn时的环境，分别对应着回调函数和wrap该函数时的上下文
        callback, contexts, args = args[0], args[1], args[2:]
        if contexts is _state.contexts or not contexts: # contexts就是当前的，或者根本就没有，那么就直接调用callback返回
            callback(*args, **kwargs)
            return
        if not _state.contexts:
            new_contexts = [cls(arg, active_cell) for (cls, arg, active_cell) in contexts if active_cell[0]]
        elif (len(_state.contexts) > len(contexts) or any(a[1] is not b[1] for a, b in itertools.izip(_state.contexts, contexts))):
            # 沿栈向上，或转到完全不同的栈中，则_state.contexts有不在contexts中的元素。使用NullContext清空状态然后重新从contexts创建
            new_contexts = [NullContext()] + [cls(arg, active_cell) for (cls, arg, active_cell) in contexts if active_cell[0]]
        else:
            # 沿栈向下，_state.contexts会是contexts的前缀。对于contexts中每一个在该前缀中的元素，生成一个新的StackContext对象
            new_contexts = [cls(arg, active_cell) for (cls, arg, active_cell) in contexts[len(_state.contexts):] if active_cell[0]]

        # 如果new_contexts列表不为空，则在new_contexts上下文环境中调用callback。基本上new_contexts都是wrap回调函数时的上下文环境。
        if len(new_contexts) > 1:
            with _nested(*new_contexts):
                callback(*args, **kwargs)
        elif new_contexts:
            with new_contexts[0]:
                callback(*args, **kwargs)
        else:
            callback(*args, **kwargs)

    if _state.contexts:
        return _StackContextWrapper(wrapped, fn, _state.contexts) # 包装时带上的是调用wrap时当前的_state.contexts
    else:
        return _StackContextWrapper(fn)


@contextlib.contextmanager
def _nested(*managers):
    """ 支持将多个contextmanager放到单个with语句中。
    Copied from the python 2.6 standard library. It's no longer present in python 3 because the with statement natively supports multiple
    context managers, but that doesn't help if the list of context managers is not known until runtime. """
    exits = []
    vars = []
    exc = (None, None, None)
    try:
        for mgr in managers:
            exit = mgr.__exit__
            enter = mgr.__enter__
            vars.append(enter()) # 依次对所有的manager调用__enter__并把结果放入vars中
            exits.append(exit)
        yield vars # 一次性把整个vars yield出去
    except:
        exc = sys.exc_info()
    finally:
        while exits:
            exit = exits.pop()
            try:
                if exit(*exc):
                    exc = (None, None, None)
            except:
                exc = sys.exc_info()
        if exc != (None, None, None):
            raise_exc_info(exc) # 不要信任sys.exc_info()依然包含正确的信息，其他的异常可能已经在某个exit方法中捕获了

