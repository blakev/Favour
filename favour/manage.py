import os
import sys
import socket
import functools
import json
import logging
import time
import uuid

from multiprocessing import Process

import tornado.netutil
import tornado.iostream
import tornado.ioloop
from tornado import gen

import errors

config = {
    'broadcast_port': 16607,
    'buffer_size': 4096 * 16,
    'recv_term_byte': 0x01
}

class Messages(object):
    ALIVE = '{Alive}'

class Member(Process):
    def __init__(self, *args, **kwargs):
        super(Member, self).__init__(*args, **kwargs)

        self.io_loop = None
        self._id = str(uuid.uuid4()).split('-')[0]
        self.args = kwargs.get('args', [])

        self.hostname = socket.gethostname()
        self.ip = socket.gethostbyname(self.hostname)
        self.bport = int(config.get('broadcast_port', 16607))

        if ':' in self.ip:
            raise NotImplementedError('IPv6 not yet implemented.')

        if self.bport < 1024:
            raise errors.PortTooLowException('Broadcast port %d is too low.' % self.bport)

        self.multicast_group = (('.'.join(self.ip.split('.')[:-1])) + '.255', self.bport)

        if not tornado.netutil.is_valid_ip(self.multicast_group[0]):
            raise errors.InvalidIPAddress(self.multicast_group[0])

        self.server_address = ('', self.bport)
        self.buffer_size = int(config.get('buffer_size', 1024))
        self.term_byte = str(config.get('recv_term_byte', 0x01))


    def listen(self, sock, fd, events):
        data = ''
        while True:
            x = sock.recv(self.buffer_size)
            data += x
            if len(x) == 0 or self.term_byte in data: break

        data = json.loads(data)
        print data

    def broadcast(self, sock, msg):
       try:
           msg = self.prepare_message(msg)
           r = sock.sendto(msg, self.multicast_group)
       except Exception:
           r = 0
           raise
       finally:
           return True if r is None else False

    def prepare_message(self, msg):
        return json.dumps(dict(
            from_ip = self.ip,
            from_host = self.hostname,
            on = self.bport,
            msg = str(msg),
            at = int(time.time()),
            src = self._id
        ))

    def run(self):
        print self.hostname, self.ip, self.multicast_group

        self.io_loop = tornado.ioloop.IOLoop.instance()

        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_sock.setblocking(0)
        udp_sock.bind(self.server_address)

        stream = tornado.iostream.IOStream(udp_sock)

        listen = functools.partial(self.listen, udp_sock)
        broadcast = functools.partial(self.broadcast, udp_sock, Messages.ALIVE)

        self.io_loop.add_handler(stream.fileno(), listen, tornado.ioloop.IOLoop.READ)
        tornado.ioloop.PeriodicCallback(broadcast, 3 * 1000, self.io_loop).start()

        self.io_loop.start()


if __name__ == '__main__':
    Member(name = 'QuorumMember', args=(1,)).start()

