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

    ## epoll模块的常量：
    _EPOLLIN = 0x001          # 普通数据可读事件
    _EPOLLPRI = 0x002         # 带外数据可读事件(未用到)
    _EPOLLOUT = 0x004         # 可写事件
    _EPOLLERR = 0x008         # 错误事件(epoll总是关注此事件)
    _EPOLLHUP = 0x010         # 异常挂断事件(epoll总是关注此事件)
    _EPOLLRDHUP = 0x2000      # 对端挂断事件(未用到)
    _EPOLLONESHOT = (1 << 30) # 设置one-shot模式(未用到)
    _EPOLLET = (1 << 31)      # 设置边缘触发模式(未用到)

    ## IOLoop的事件映射
    NONE = 0
    READ = _EPOLLIN
    WRITE = _EPOLLOUT
    ERROR = _EPOLLERR | _EPOLLHUP

    ## 保证ioloop的全局唯一单例
    _instance_lock = threading.Lock()

    def __init__(self, impl=None):
        self._impl = impl or _poll()           # Linux下即epoll
        if hasattr(self._impl, 'fileno'):      # 若支持，设置FD_CLOEXEC
            set_close_exec(self._impl.fileno())

        self._callback_lock = threading.Lock() # 使self._callbacks可用于多线程

        self._handlers = {}      # epoll中每个fd的处理函数Map
        self._events = {}        # epoll返回的待处理事件Map
        self._callbacks = []     # 用户加入的回调函数列表
        self._timeouts = []      # ioloop中基于时间的调度，是一个小根堆

        self._running = False      # 标记ioloop已经调用了start，还未调用stop
        self._stopped = False      # 标记ioloop循环已退出，或已调用了stop
        self._thread_ident = None  # 标记ioloop所运行的线程，以支持多线程访问
        self._blocking_signal_threshold = None

        self._waker = Waker()    # 创建一个管道，用于在其他线程调用add_callback时唤醒epoll_wait
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
        """ 关闭ioloop，并释放所有使用到的资源。关闭之前必须先stop。 """
        self.remove_handler(self._waker.fileno())
        if all_fds:            # 如果all_fds是True，则同时关闭所有注册到该ioloop上的文件描述符
            for fd in self._handlers.keys()[:]:
                try:
                    os.close(fd)
                except Exception:
                    logging.debug("error closing fd %s", fd, exc_info=True)
        self._waker.close()    # 释放_waker管道
        self._impl.close()     # 释放epoll实例

    def add_handler(self, fd, handler, events):
        """ 为给定的文件描述符fd注册events事件的处理函数handler。 """
        self._handlers[fd] = stack_context.wrap(handler) # 在_handlers字典中以fd为键加入handler处理函数
        self._impl.register(fd, events | self.ERROR)     # 在epoll实例上为fd注册感兴趣的事件events|ERROR

    def update_handler(self, fd, events):
        """ 改变给定的文件描述符fd感兴趣的事件。 """
        self._impl.modify(fd, events | self.ERROR)

    def remove_handler(self, fd):
        """ 移除给定的文件描述符的事件处理。 """
        self._handlers.pop(fd, None)  # 把fd及其对应的handler从_handlers中移除
        self._events.pop(fd, None)    # 把fd及其对应的未处理事件从_events中移除
        try:
            self._impl.unregister(fd) # 在epoll实例上移动文件描述符fd的注册
        except (OSError, IOError):
            logging.debug("Error deleting fd from IOLoop", exc_info=True)

    def set_blocking_signal_threshold(self, seconds, action):
        """ 当ioloop阻塞超过seconds秒之后，发送一个信号。若seconds=None则不发送信号。 """
        if not hasattr(signal, "setitimer"):
            logging.error("set_blocking_signal_threshold requires a signal module with the setitimer method")
            return
        self._blocking_signal_threshold = seconds # 记下秒数，闹钟不在此处设置
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
        """ 开始IO事件循环。开始后，会运行直到调用了stop方法，这将使得循环在处理完当前事件之后停止。 """
        if self._stopped:
            self._stopped = False
            return
        self._thread_ident = thread.get_ident() # 记录loop所在线程id，以判断add_callback是否是在loop线程
        self._running = True
        while True:
            poll_timeout = 3600.0 # epoll_wait超时时间，用于时间调度，若没有self._timeout则默认1小时

            ## 将新产生的callback推迟到下一轮loop中调用，以防止IO事件饥饿
            with self._callback_lock:
                callbacks = self._callbacks
                self._callbacks = []
            for callback in callbacks:
                self._run_callback(callback)

            ## 基于时间的调度：不断从self._timeouts这个小根堆中取出deadline最早的timeout任务，
            ## 若deadline已到，则马上调用其callback；否则，重新调整poll_timeout以确保下次loop时能调用该timeout。
            if self._timeouts:
                now = time.time()
                while self._timeouts:
                    if self._timeouts[0].callback is None:
                        heapq.heappop(self._timeouts)
                    elif self._timeouts[0].deadline <= now:
                        timeout = heapq.heappop(self._timeouts)
                        self._run_callback(timeout.callback)
                    else:
                        seconds = self._timeouts[0].deadline - now
                        poll_timeout = min(seconds, poll_timeout)
                        break

            ## 如果在处理callbacks和timeouts的时候又加入了新的callback，则epoll_wait不等待，以免阻塞了callbacks
            if self._callbacks:
                poll_timeout = 0.0

            ## 检查运行标志。如果在处理callbacks和timeouts的时候调用了stop方法，则退出循环
            if not self._running:
                break

            ## 清空闹钟，使它在epoll_wait时不要发送
            if self._blocking_signal_threshold is not None:
                signal.setitimer(signal.ITIMER_REAL, 0, 0)

            ## 调用epoll_wait以等待IO事件的发生
            try:
                event_pairs = self._impl.poll(poll_timeout)
            except Exception, e:
                if (getattr(e, 'errno', None) == errno.EINTR or
                    (isinstance(getattr(e, 'args', None), tuple) and len(e.args) == 2 and e.args[0] == errno.EINTR)):
                    continue
                else:
                    raise

            ## 恢复闹钟
            if self._blocking_signal_threshold is not None:
                signal.setitimer(signal.ITIMER_REAL, self._blocking_signal_threshold, 0)

            ## 此时epoll_wait已经返回，将返回的IO事件加入到self._events中去
            self._events.update(event_pairs)

            # Since that handler may perform actions on other file descriptors,
            # there may be reentrant calls to this IOLoop that update self._events
            ## 开始处理self._events，其中每一个元素都是一个(fd, events)，events是发生的事件
            ## 通过之前注册的self._handlers[fd]来处理fd对应的events
            while self._events:
                fd, events = self._events.popitem()
                try:
                    self._handlers[fd](fd, events)
                except (OSError, IOError), e:
                    if e.args[0] == errno.EPIPE: # 客户端关闭了连接
                        pass
                    else:
                        logging.error("Exception in I/O handler for fd %s", fd, exc_info=True)
                except Exception:
                    logging.error("Exception in I/O handler for fd %s", fd, exc_info=True)
            ## 处理完成后，进入到下一轮loop

        ## loop已经退出，即已经调用了stop
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
        timeout = _Timeout(deadline, stack_context.wrap(callback))
        heapq.heappush(self._timeouts, timeout)
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
            self._waker.wake() # 如果是在非IOLoop线程中加入callback到了一个空_callbacks集合中，则试图唤醒IOLoop

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

