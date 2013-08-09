# vim: fileencoding=utf-8

import socket

from tornado import ioloop
import iostream


class Client(object):

    def __init__(self, domain, uri):
        self.domain = domain
        self.uri = uri
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.stream = iostream.IOStream(s)

    def open(self):
        self.stream.connect((self.domain, 80), self.send_request)

    def on_chunk_read(self, chunk):
        print '*'*15, 'streaming callback', '*'*15+'\n', chunk

    def on_bytes_read(self, chunk):
        print '*'*15, 'bytes callback', '*'*15+'\n', chunk
        #self.stream.read_bytes(1024, self.on_bytes_read, self.on_chunk_read)
        self.stream.read_until_close(self.on_close, self.on_chunk_read)

    def on_close(self, res):
        print '*'*15, 'close callback', '*'*15+'\n', res
        self.stream.close()
        ioloop.IOLoop.instance().stop()

    def send_request(self):
        self.stream.write('GET %s HTTP/1.1\r\nHost: %s\r\n\r\n'
                % (self.uri, self.domain))
        return
        #self.stream.read_until_close(self.on_close, self.on_chunk_read)
        self.stream.read_bytes(4096, self.on_bytes_read)

client = Client('www.baidu.com', '/')
client.open()

ioloop.IOLoop.instance().start()

