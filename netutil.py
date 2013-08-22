# vim: fileencoding=utf-8

from __future__ import absolute_import, division, with_statement

import errno
import logging
import os
import socket
import stat

from tornado import process
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream, SSLIOStream
from tornado.platform.auto import set_close_exec

try:
    import ssl  # Python 2.6+
except ImportError:
    ssl = None


class TCPServer(object):
    """ 非阻塞、单线程的TCP服务器。 """
    def __init__(self, io_loop=None, ssl_options=None):
        self.io_loop = io_loop
        self.ssl_options = ssl_options
        self._sockets = {}  # fd -> socket object
        self._pending_sockets = []
        self._started = False

        # Verify the SSL options. Otherwise we don't get errors until clients connect.
        # This doesn't verify that the keys are legitimate, but the SSL module doesn't do
        # that until there is a connected socket which seems like too much work
        if self.ssl_options is not None:
            # Only certfile is required: it can contain both keys
            if 'certfile' not in self.ssl_options:
                raise KeyError('missing key "certfile" in ssl_options')
            if not os.path.exists(self.ssl_options['certfile']):
                raise ValueError('certfile "%s" does not exist' % self.ssl_options['certfile'])
            if ('keyfile' in self.ssl_options and not os.path.exists(self.ssl_options['keyfile'])):
                raise ValueError('keyfile "%s" does not exist' % self.ssl_options['keyfile'])

    def listen(self, port, address=""):
        """ 在指定的端口上开始接受连接。该方法可以调用多次，以监听多个端口。该方法立即生效，无需再调用TCPServer.start。 """
        sockets = bind_sockets(port, address=address)
        self.add_sockets(sockets)

    def add_sockets(self, sockets):
        """ 使该服务器开始在指定的sockets上接受连接。 """
        if self.io_loop is None:
            self.io_loop = IOLoop.instance()
        for sock in sockets:
            self._sockets[sock.fileno()] = sock
            add_accept_handler(sock, self._handle_connection, io_loop=self.io_loop)

    def add_socket(self, socket):
        """ Singular version of `add_sockets`.  Takes a single socket object. """
        self.add_sockets([socket])

    def bind(self, port, address=None, family=socket.AF_UNSPEC, backlog=128):
        """ Binds this server to the given port on the given address.

        To start the server, call `start`. If you want to run this server
        in a single process, you can call `listen` as a shortcut to the
        sequence of `bind` and `start` calls.

        Address may be either an IP address or hostname.  If it's a hostname,
        the server will listen on all IP addresses associated with the
        name.  Address may be an empty string or None to listen on all
        available interfaces.  Family may be set to either ``socket.AF_INET``
        or ``socket.AF_INET6`` to restrict to ipv4 or ipv6 addresses, otherwise
        both will be used if available.

        The ``backlog`` argument has the same meaning as for
        `socket.listen`.

        This method may be called multiple times prior to `start` to listen
        on multiple ports or interfaces.
        """
        sockets = bind_sockets(port, address=address, family=family, backlog=backlog)
        if self._started:
            self.add_sockets(sockets)
        else:
            self._pending_sockets.extend(sockets)

    def start(self, num_processes=1):
        """Starts this server in the IOLoop.

        By default, we run the server in this process and do not fork any
        additional child process.

        If num_processes is ``None`` or <= 0, we detect the number of cores
        available on this machine and fork that number of child
        processes. If num_processes is given and > 1, we fork that
        specific number of sub-processes.

        Since we use processes and not threads, there is no shared memory
        between any server code.

        Note that multiple processes are not compatible with the autoreload
        module (or the ``debug=True`` option to `tornado.web.Application`).
        When using multiple processes, no IOLoops can be created or
        referenced until after the call to ``TCPServer.start(n)``.
        """
        assert not self._started
        self._started = True
        if num_processes != 1:
            process.fork_processes(num_processes)
        sockets = self._pending_sockets
        self._pending_sockets = []
        self.add_sockets(sockets)

    def stop(self):
        """Stops listening for new connections.

        Requests currently in progress may still continue after the
        server is stopped.
        """
        for fd, sock in self._sockets.iteritems():
            self.io_loop.remove_handler(fd)
            sock.close()

    def handle_stream(self, stream, address):
        """Override to handle a new `IOStream` from an incoming connection."""
        raise NotImplementedError()

    def _handle_connection(self, connection, address): # connection, address是socket.accept()的返回结果
        if self.ssl_options is not None:
            assert ssl, "Python 2.6+ and OpenSSL required for SSL"
            try:
                connection = ssl.wrap_socket(connection, server_side=True, do_handshake_on_connect=False, **self.ssl_options)
            except ssl.SSLError, err:
                if err.args[0] == ssl.SSL_ERROR_EOF:
                    return connection.close()
                else:
                    raise
            except socket.error, err:
                if err.args[0] == errno.ECONNABORTED:
                    return connection.close()
                else:
                    raise
        try:
            if self.ssl_options is not None:
                stream = SSLIOStream(connection, io_loop=self.io_loop)
            else:
                stream = IOStream(connection, io_loop=self.io_loop) # 把新的socket包装成一个stream
            self.handle_stream(stream, address)
        except Exception:
            logging.error("Error in connection callback", exc_info=True)


def bind_sockets(port, address=None, family=socket.AF_UNSPEC, backlog=128):
    """ 创建绑定到指定端口和地址的监听sockets，返回socket对象的一个list。 """
    sockets = []
    if address == "":
        address = None
    flags = socket.AI_PASSIVE # server socket
    if hasattr(socket, "AI_ADDRCONFIG"):
        # AI_ADDRCONFIG ensures that we only try to bind on ipv6 if the system is configured for it,
        # but the flag doesn't exist on some platforms (specifically WinXP, although newer versions of windows have it)
        flags |= socket.AI_ADDRCONFIG
    for res in set(socket.getaddrinfo(address, port, family, socket.SOCK_STREAM, 0, flags)):
        af, socktype, proto, canonname, sockaddr = res
        sock = socket.socket(af, socktype, proto)
        set_close_exec(sock.fileno())
        if os.name != 'nt':
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if af == socket.AF_INET6:
            # On linux, ipv6 sockets accept ipv4 too by default, but this makes it impossible to bind to both
            # 0.0.0.0 in ipv4 and :: in ipv6.  On other systems, separate sockets *must* be used to listen for both ipv4
            # and ipv6.  For consistency, always disable ipv4 on our ipv6 sockets and use a separate ipv4 socket when needed.
            #
            # Python 2.x on windows doesn't have IPPROTO_IPV6.
            if hasattr(socket, "IPPROTO_IPV6"):
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        sock.setblocking(0)
        sock.bind(sockaddr)
        sock.listen(backlog) # 开始监听，非阻塞
        sockets.append(sock)
    return sockets

if hasattr(socket, 'AF_UNIX'):
    def bind_unix_socket(file, mode=0600, backlog=128):
        """Creates a listening unix socket.

        If a socket with the given name already exists, it will be deleted.
        If any other file with that name exists, an exception will be
        raised.

        Returns a socket object (not a list of socket objects like
        `bind_sockets`)
        """
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        set_close_exec(sock.fileno())
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(0)
        try:
            st = os.stat(file)
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise
        else:
            if stat.S_ISSOCK(st.st_mode):
                os.remove(file)
            else:
                raise ValueError("File %s exists and is not a socket", file)
        sock.bind(file)
        os.chmod(file, mode)
        sock.listen(backlog)
        return sock


def add_accept_handler(sock, callback, io_loop=None):
    """ 添加一个IOLoop事件handler以处理该sock上的新连接。 """
    if io_loop is None:
        io_loop = IOLoop.instance()
    def accept_handler(fd, events):
        while True:
            try:
                connection, address = sock.accept()
            except socket.error, e:
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    return # 当前没有连接（即建立连接会阻塞），则直接返回
                raise
            callback(connection, address) # 建立连接后以(connection, address)来调用回调函数
    io_loop.add_handler(sock.fileno(), accept_handler, IOLoop.READ)
