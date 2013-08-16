# vim: fileencoding=utf-8

from __future__ import absolute_import, division, with_statement

import datetime
import errno
import heapq
import os
import logging
import select
import thread
import threading
import time
import traceback

from tornado import stack_context

try:
    import signal
except ImportError:
    signal = None

from tornado.platform.auto import set_close_exec, Waker


class IOLoop(object):

    """ 水平触发的epoll(只考虑Linux)实现的IO Loop。 """

    # epoll模块的常量
    _EPOLLIN = 0x001
    _EPOLLPRI = 0x002 # 未用到?
    _EPOLLOUT = 0x004
    _EPOLLERR = 0x008
    _EPOLLHUP = 0x010
    _EPOLLRDHUP = 0x2000 # 未用到?
    _EPOLLONESHOT = (1 << 30) # 未用到?
    _EPOLLET = (1 << 31) # 未用到?

    # IOLoop的事件映射
    NONE = 0
    READ = _EPOLLIN # 1
    WRITE = _EPOLLOUT # 4
    ERROR = _EPOLLERR | _EPOLLHUP # 24

    # 生成全局ioloop对象的锁
    _instance_lock = threading.Lock()

    def __init__(self, impl=None):
        self._impl = impl or _poll() # self._impl默认是select.epoll()
        if hasattr(self._impl, 'fileno'):
            set_close_exec(self._impl.fileno())

        self._callback_lock = threading.Lock() # 回调锁

        self._handlers = {} # 处理函数集合
        self._events = {}
        self._callbacks = [] # 回调函数集合
        self._timeouts = [] # 基于时间的调度，_timeouts是个堆

        self._running = False # 运行标志
        self._stopped = False
        self._thread_ident = None
        self._blocking_signal_threshold = None

        # 创建一个管道，当我们想在ioloop空闲时唤醒它就通过管道发送假的数据
        self._waker = Waker()
        self.add_handler(self._waker.fileno(), lambda fd, events: self._waker.consume(), self.READ)

    @staticmethod
    def instance():
        """ 返回全局ioloop单例。 """
        if not hasattr(IOLoop, "_instance"):
            with IOLoop._instance_lock:
                if not hasattr(IOLoop, "_instance"):
                    IOLoop._instance = IOLoop()
        return IOLoop._instance

    @staticmethod
    def initialized():
        """ 全局ioloop单例是否已经生成。 """
        return hasattr(IOLoop, "_instance")

    def install(self):
        """ 将当前的ioloop注册为全局ioloop单例。用于子类。 """
        assert not IOLoop.initialized()
        IOLoop._instance = self

    def close(self, all_fds=False):
        """ 关闭ioloop，并释放所有使用到的资源。关闭之前必须先stop。
        如果all_fds是True，则同时关闭所有注册到该ioloop上的文件描述符。 """
        self.remove_handler(self._waker.fileno())
        if all_fds:
            for fd in self._handlers.keys()[:]:
                try:
                    os.close(fd)
                except Exception:
                    logging.debug("error closing fd %s", fd, exc_info=True)
        self._waker.close() # 释放_waker管道
        self._impl.close() # 释放epoll实例

    def add_handler(self, fd, handler, events):
        """ 为给定的文件描述符fd注册events事件的处理函数handler。 """
        self._handlers[fd] = stack_context.wrap(handler) # 在_handlers字典中以fd为键加入handler处理函数
        self._impl.register(fd, events | self.ERROR) # 在epoll实例上为fd注册感兴趣的事件events|ERROR

    def update_handler(self, fd, events):
        """ 改变给定的文件描述符fd感兴趣的事件。 """
        self._impl.modify(fd, events | self.ERROR)

    def remove_handler(self, fd):
        """ 移除给定的文件描述符的事件处理。 """
        self._handlers.pop(fd, None) # 把fd及其对应的handler从_handlers中移除
        self._events.pop(fd, None) # 把fd及其对应的未处理事件从_events中移除
        try:
            self._impl.unregister(fd) # 在epoll实例上移动文件描述符fd的注册
        except (OSError, IOError):
            logging.debug("Error deleting fd from IOLoop", exc_info=True)

    def set_blocking_signal_threshold(self, seconds, action):
        """ 当ioloop阻塞超过seconds秒之后，发送一个信号。若seconds=None则不发送信号。 """
        if not hasattr(signal, "setitimer"):
            logging.error("set_blocking_signal_threshold requires a signal module with the setitimer method")
            return
        self._blocking_signal_threshold = seconds # 先记下秒数，闹钟不在此处设置
        if seconds is not None:
            signal.signal(signal.SIGALRM, action if action is not None else signal.SIG_DFL) # 设置信号处理函数

    def set_blocking_log_threshold(self, seconds):
        """ 当ioloop阻塞超过seconds秒之后，log一下stack。若seconds=None则不发送信号。 """
        self.set_blocking_signal_threshold(seconds, self.log_stack)

    def log_stack(self, signal, frame):
        """ 信号处理函数。记录当前线程的stack trace。与set_blocking_signal_threshold一起使用。 """
        logging.warning('IOLoop blocked for %f seconds in\n%s',
                self._blocking_signal_threshold, ''.join(traceback.format_stack(frame)))

    def start(self):
        """ 开始IO事件循环。
        IO事件循环开始后，会一直运行到某一个handler调用了stop方法，这将使得循环在处理完当前事件之后停止。 """
        if self._stopped:
            self._stopped = False
            return
        self._thread_ident = thread.get_ident() # 记录IOLoop所在的线程的id，用来判断操作是否在IOLoop线程
        self._running = True
        while True:
            poll_timeout = 3600.0 # epoll的等待超时时间

            # 将新的callback推迟到下一轮事件循环中调用，以防止IO事件饥饿
            with self._callback_lock:
                callbacks = self._callbacks
                self._callbacks = [] # 新的callback会加入到这个空的self._callbacks中
            for callback in callbacks: # 只运行callbacks里的callback
                self._run_callback(callback)

            if self._timeouts: # 基于时间的调度
                now = time.time()
                while self._timeouts: # _timeouts[0]是timeout值最小的
                    if self._timeouts[0].callback is None: # 该timeout已经取消，直接pop掉
                        heapq.heappop(self._timeouts)
                    elif self._timeouts[0].deadline <= now: # 该timeout已经到点，赶紧pop出来调用
                        timeout = heapq.heappop(self._timeouts)
                        self._run_callback(timeout.callback)
                    else: # 还没有到点的timeout，则把epoll的等待时间阈值设为最近要到点的timeout还剩下的时间如果它比当前值更短的话
                        seconds = self._timeouts[0].deadline - now
                        poll_timeout = min(seconds, poll_timeout)
                        break

            if self._callbacks: # 如果在处理callbacks和timeouts的时候又加入了新的callback，则epoll不等待，以免callback也一起等待
                poll_timeout = 0.0

            if not self._running: # 检查运行标志。如果在处理callbacks和timeouts的时候调用了stop方法，则退出循环
                break

            if self._blocking_signal_threshold is not None:
                signal.setitimer(signal.ITIMER_REAL, 0, 0) # 清空闹钟，使它在epoll在等待时不要发送

            try:
                event_pairs = self._impl.poll(poll_timeout) # 等待IO事件
            except Exception, e:
                if (getattr(e, 'errno', None) == errno.EINTR or
                    (isinstance(getattr(e, 'args', None), tuple) and len(e.args) == 2 and e.args[0] == errno.EINTR)):
                    # epoll的poll操作等待超时，或者被信号处理函数打断，需要手动重启
                    continue
                else: # 其他异常不做处理，直接抛出
                    raise

            if self._blocking_signal_threshold is not None: # 恢复闹钟
                signal.setitimer(signal.ITIMER_REAL, self._blocking_signal_threshold, 0)

            self._events.update(event_pairs) # 将等待到的IO事件加入到_events中去

            # Since that handler may perform actions on other file descriptors,
            # there may be reentrant calls to this IOLoop that update self._events
            # 每次从_events中pop出一个fd，然后运行它对应的handler。
            while self._events:
                fd, events = self._events.popitem()
                try:
                    self._handlers[fd](fd, events)
                except (OSError, IOError), e:
                    if e.args[0] == errno.EPIPE: # Happens when the client closes the connection
                        pass
                    else:
                        logging.error("Exception in I/O handler for fd %s", fd, exc_info=True)
                except Exception:
                    logging.error("Exception in I/O handler for fd %s", fd, exc_info=True)

        # 退出循环后重置stopped标志，使其可以重新开始
        self._stopped = False
        if self._blocking_signal_threshold is not None: # 清除闹钟
            signal.setitimer(signal.ITIMER_REAL, 0, 0)

    def stop(self):
        """ 在下一轮IO循环中停止循环。
        如果循环当前没有运行，则下一次调用start()会马上返回。 """
        self._running = False # 设置运行标志 
        self._stopped = True # 使start()马上返回
        self._waker.wake() # 防止循环一直在等待中，不能马上退出

    def running(self):
        """ 返回当前IOLoop是否正在运行。 """
        return self._running

    def add_timeout(self, deadline, callback):
        """ 在IOLoop中，当deadline到点时调用callback。返回一个可用于取消的句柄。
        在其他线程调用该方法不安全，应该在IOLoop线程中添加（利用add_callback方法）。 """
        timeout = _Timeout(deadline, stack_context.wrap(callback)) # _Timeout对象
        heapq.heappush(self._timeouts, timeout) # _timeouts是个堆
        return timeout

    def remove_timeout(self, timeout):
        """ 取消一个pending的timeout。 """
        timeout.callback = None # _timeouts是个堆，这里只简单地把callback设置为None，具体的移除还是在IOLoop中

    def add_callback(self, callback):
        """ 在下一轮IOLoop中调用给定的callback。
        这是唯一一个在任何时间、任何线程都能安全调用的方法。其他的操作都应该使用该方法加入到IOLoop中。 """
        with self._callback_lock:
            list_empty = not self._callbacks
            self._callbacks.append(stack_context.wrap(callback))
        if list_empty and thread.get_ident() != self._thread_ident:
            # 如果是在非IOLoop线程中加入callback到了一个空_callbacks集合中，则试图唤醒IOLoop
            self._waker.wake()

    def _run_callback(self, callback):
        try:
            callback() # 直接调用callback
        except Exception:
            self.handle_callback_exception(callback)

    def handle_callback_exception(self, callback):
        """ 当IOLoop运行callback抛异常时，该方法被调用。
        默认只是log一下异常栈，子类可重载实现。异常对象可由sys.exc_info得到。 """
        logging.error("Exception in callback %r", callback, exc_info=True)


class _Timeout(object):
    """An IOLoop timeout, a UNIX timestamp and a callback"""

    # Reduce memory overhead when there are lots of pending callbacks
    __slots__ = ['deadline', 'callback']

    def __init__(self, deadline, callback):
        if isinstance(deadline, (int, long, float)):
            self.deadline = deadline
        elif isinstance(deadline, datetime.timedelta):
            self.deadline = time.time() + _Timeout.timedelta_to_seconds(deadline)
        else:
            raise TypeError("Unsupported deadline %r" % deadline)
        self.callback = callback

    @staticmethod
    def timedelta_to_seconds(td):
        """Equivalent to td.total_seconds() (introduced in python 2.7)."""
        return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / float(10 ** 6)

    # Comparison methods to sort by deadline, with object id as a tiebreaker
    # to guarantee a consistent ordering.  The heapq module uses __le__
    # in python2.5, and __lt__ in 2.6+ (sort() and most other comparisons
    # use __lt__).
    def __lt__(self, other):
        return ((self.deadline, id(self)) <
                (other.deadline, id(other)))

    def __le__(self, other):
        return ((self.deadline, id(self)) <=
                (other.deadline, id(other)))


class PeriodicCallback(object):
    """Schedules the given callback to be called periodically.

    The callback is called every callback_time milliseconds.

    `start` must be called after the PeriodicCallback is created.
    """
    def __init__(self, callback, callback_time, io_loop=None):
        self.callback = callback
        self.callback_time = callback_time
        self.io_loop = io_loop or IOLoop.instance()
        self._running = False
        self._timeout = None

    def start(self):
        """Starts the timer."""
        self._running = True
        self._next_timeout = time.time()
        self._schedule_next()

    def stop(self):
        """Stops the timer."""
        self._running = False
        if self._timeout is not None:
            self.io_loop.remove_timeout(self._timeout)
            self._timeout = None

    def _run(self):
        if not self._running:
            return
        try:
            self.callback()
        except Exception:
            logging.error("Error in periodic callback", exc_info=True)
        self._schedule_next()

    def _schedule_next(self):
        if self._running:
            current_time = time.time()
            while self._next_timeout <= current_time:
                self._next_timeout += self.callback_time / 1000.0
            self._timeout = self.io_loop.add_timeout(self._next_timeout, self._run)


class _EPoll(object):
    """An epoll-based event loop using our C module for Python 2.5 systems"""
    _EPOLL_CTL_ADD = 1
    _EPOLL_CTL_DEL = 2
    _EPOLL_CTL_MOD = 3

    def __init__(self):
        self._epoll_fd = epoll.epoll_create()

    def fileno(self):
        return self._epoll_fd

    def close(self):
        os.close(self._epoll_fd)

    def register(self, fd, events):
        epoll.epoll_ctl(self._epoll_fd, self._EPOLL_CTL_ADD, fd, events)

    def modify(self, fd, events):
        epoll.epoll_ctl(self._epoll_fd, self._EPOLL_CTL_MOD, fd, events)

    def unregister(self, fd):
        epoll.epoll_ctl(self._epoll_fd, self._EPOLL_CTL_DEL, fd, 0)

    def poll(self, timeout):
        return epoll.epoll_wait(self._epoll_fd, int(timeout * 1000))


class _KQueue(object):
    """A kqueue-based event loop for BSD/Mac systems."""
    def __init__(self):
        self._kqueue = select.kqueue()
        self._active = {}

    def fileno(self):
        return self._kqueue.fileno()

    def close(self):
        self._kqueue.close()

    def register(self, fd, events):
        if fd in self._active:
            raise IOError("fd %d already registered" % fd)
        self._control(fd, events, select.KQ_EV_ADD)
        self._active[fd] = events

    def modify(self, fd, events):
        self.unregister(fd)
        self.register(fd, events)

    def unregister(self, fd):
        events = self._active.pop(fd)
        self._control(fd, events, select.KQ_EV_DELETE)

    def _control(self, fd, events, flags):
        kevents = []
        if events & IOLoop.WRITE:
            kevents.append(select.kevent(
                    fd, filter=select.KQ_FILTER_WRITE, flags=flags))
        if events & IOLoop.READ or not kevents:
            # Always read when there is not a write
            kevents.append(select.kevent(
                    fd, filter=select.KQ_FILTER_READ, flags=flags))
        # Even though control() takes a list, it seems to return EINVAL
        # on Mac OS X (10.6) when there is more than one event in the list.
        for kevent in kevents:
            self._kqueue.control([kevent], 0)

    def poll(self, timeout):
        kevents = self._kqueue.control(None, 1000, timeout)
        events = {}
        for kevent in kevents:
            fd = kevent.ident
            if kevent.filter == select.KQ_FILTER_READ:
                events[fd] = events.get(fd, 0) | IOLoop.READ
            if kevent.filter == select.KQ_FILTER_WRITE:
                if kevent.flags & select.KQ_EV_EOF:
                    # If an asynchronous connection is refused, kqueue
                    # returns a write event with the EOF flag set.
                    # Turn this into an error for consistency with the
                    # other IOLoop implementations.
                    # Note that for read events, EOF may be returned before
                    # all data has been consumed from the socket buffer,
                    # so we only check for EOF on write events.
                    events[fd] = IOLoop.ERROR
                else:
                    events[fd] = events.get(fd, 0) | IOLoop.WRITE
            if kevent.flags & select.KQ_EV_ERROR:
                events[fd] = events.get(fd, 0) | IOLoop.ERROR
        return events.items()


class _Select(object):
    """A simple, select()-based IOLoop implementation for non-Linux systems"""
    def __init__(self):
        self.read_fds = set()
        self.write_fds = set()
        self.error_fds = set()
        self.fd_sets = (self.read_fds, self.write_fds, self.error_fds)

    def close(self):
        pass

    def register(self, fd, events):
        if fd in self.read_fds or fd in self.write_fds or fd in self.error_fds:
            raise IOError("fd %d already registered" % fd)
        if events & IOLoop.READ:
            self.read_fds.add(fd)
        if events & IOLoop.WRITE:
            self.write_fds.add(fd)
        if events & IOLoop.ERROR:
            self.error_fds.add(fd)
            # Closed connections are reported as errors by epoll and kqueue,
            # but as zero-byte reads by select, so when errors are requested
            # we need to listen for both read and error.
            self.read_fds.add(fd)

    def modify(self, fd, events):
        self.unregister(fd)
        self.register(fd, events)

    def unregister(self, fd):
        self.read_fds.discard(fd)
        self.write_fds.discard(fd)
        self.error_fds.discard(fd)

    def poll(self, timeout):
        readable, writeable, errors = select.select(
            self.read_fds, self.write_fds, self.error_fds, timeout)
        events = {}
        for fd in readable:
            events[fd] = events.get(fd, 0) | IOLoop.READ
        for fd in writeable:
            events[fd] = events.get(fd, 0) | IOLoop.WRITE
        for fd in errors:
            events[fd] = events.get(fd, 0) | IOLoop.ERROR
        return events.items()


# 选择poll的实现。优先使用epoll/kqueu，如果没有则回到select上。
if hasattr(select, "epoll"):
    _poll = select.epoll # Python 2.6+ on Linux
elif hasattr(select, "kqueue"):
    _poll = _KQueue # Python 2.6+ on BSD or Mac
else:
    try:
        from tornado import epoll # 使用我们自己的C模块
        _poll = _EPoll
    except Exception: # 实在是没有epoll了
        import sys
        if "linux" in sys.platform:
            logging.warning("epoll module not found; using select()")
        _poll = _Select # 性能低下

