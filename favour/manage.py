import os
import sys
import socket
import functools
import json
import logging
import time
import uuid
import math
import errno

from collections import namedtuple
from multiprocessing import Process

import tornado.netutil
import tornado.iostream
import tornado.ioloop
import tornado.web

import errors

config = {
    'broadcast_interval': 3, # seconds
    'broadcast_port': 16607,
    'buffer_size': 4096 * 16,
    'find_leader_every': 1, # seconds
    'force_delim': '!!',
    'member_expires_after': 5, # seconds
    'recv_term_byte': 0x01,
    'vote_duration': 2, # seconds
    'vote_threshold': 0.50, # percent
    'www_port': 6697
}

class Messages(object):
    FORCE = config['force_delim'] + '%s'

    ACK = 'ACK'         # acknowledge heartbeat
    BCK = 'BCK:%s'      # back-acknowledge heartbeal

    ALL = 'ALL:(%s)'    # return list of known members

    LDR = 'LDR:%s'      # vote on new leader
    DRP = 'DRP:%s'      # vote drop on member by _id

    CLR = 'CLR:%s'      # clear vote of %s; leader issued

    Votable = ['DRP']
    Community = ['LDR']


MemberId = namedtuple('MemberId', 'ip hostname src')

VOTING_DICT = {
    'started': None,
    'action': None,
    'votes': {}}


class Member(Process):
    def __init__(self, *args, **kwargs):
        super(Member, self).__init__(*args, **kwargs)

        self.io_loop = None
        self._id = hex(uuid.uuid4().fields[0])
        self.args = kwargs.get('args', [])

        self.bport = int(config['broadcast_port'])
        self.hostname = socket.gethostname()
        self.ip = socket.gethostbyname(self.hostname)

        if ':' in self.ip:
            raise NotImplementedError('IPv6 not yet implemented.')

        if self.bport < 1024:
            raise errors.PortTooLowException('Broadcast port %d is too low.' % self.bport)

        self.multicast_group = (('.'.join(self.ip.split('.')[:-1])) + '.255', self.bport)

        if not tornado.netutil.is_valid_ip(self.multicast_group[0]):
            raise errors.InvalidIPAddress(self.multicast_group[0])

        self.server_address = ('', self.bport)

        self.buffer_size = int(config['buffer_size'])
        self.term_byte = str(config['recv_term_byte'])

        self.members = {}
        self.leader = None
        self.last_ack = None

        self.votes = {}


    @property
    def isleader(self):
        return self._id == self.leader


    def __drop_expired_members(self, sock):
        if not self.members:
            return
        else:
            for mem, data in self.members.items():
                if data['at'] + config['member_expires_after'] < time.time():
                    self.broadcast(sock, Messages.DRP % str(mem.src))


    def __find_leader(self, sock):
        if not self.members:
            self.leader = self._id
        else:
            temp_members = [k.src for k in self.members.keys() if self.members[k]['at'] + config['member_expires_after'] > time.time()]
            possible_leader = min(temp_members)

            if not self.leader or possible_leader != self.leader:
                self.broadcast(sock, Messages.LDR % possible_leader)


    def __handle_udp_data(self, sock, data):
        data = dict(data)
        data['at'] = time.time()

        _from = MemberId(data['from_ip'], data['from_host'], data['src'])

        if _from not in self.members.keys():
            self.broadcast(sock, Messages.BCK % _from.src)

        if data['leader']:
            self.leader = _from.src

        self.members[_from] = data

        data = data['msg'].split(':')
        command, msg = data[0], ':'.join(data[1:])

        print _from.ip, command, len(self.members.keys()), msg

        if msg == Messages.ACK:
            return

        if command.startswith(config['force_delim']): # force this action
            command = command.strip(config['force_delim'])

            if command == 'DRP':
                for mem, data in self.members.items():
                    if mem.src == msg:
                        del self.members[mem]
                    if mem.ip == self.ip:
                        self.io_loop.add_callback(self.launch_http_interface)


        elif command in Messages.Community:
            if command == 'LDR':
                self.leader = msg

        elif command in Messages.Votable: # vote on the action
            if not self.isleader:
                return

            v = self.votes.setdefault(msg, dict(VOTING_DICT))

            if v['started'] is None:
                v['started'] = time.time()

            v['action'] = command

            if v['started'] + config['vote_duration'] < time.time(): # make decision
                if len(v['votes'].keys()) >= math.floor(len(self.members.keys()) * config['vote_threshold']): # do it!
                    self.force_broadcast(sock, Messages.__dict__[v['action']] % str(msg))
                del self.votes[msg]

            else:
                v['votes'][_from] = True



    def listen(self, sock, fd, events):
        data = ''
        while True:
            x = sock.recv(self.buffer_size)
            data += x
            if len(x) == 0 or self.term_byte in data: break
        self.__handle_udp_data(sock, json.loads(data))


    def force_broadcast(self, sock, msg):
        if not self.isleader:
            return self.broadcast(sock, msg)
        else:
            return self.broadcast(sock, Messages.FORCE % msg)


    def broadcast(self, sock, msg):
        r = 0
        try:
           msg = self.prepare_message(msg)
           r = sock.sendto(msg, self.multicast_group)
           self.last_ack = time.time()
        except Exception:
           raise
        finally:
           return True if r is None else False


    def prepare_message(self, msg):
        return json.dumps(dict(
            from_ip = self.ip,
            from_host = self.hostname,
            on = self.bport,
            msg = str(msg),
            src = self._id,
            leader = self.isleader))

    def launch_http_interface(self):
        try:
            web_application = tornado.web.Application([
                (r'/', MainHandler)
            ])

            web_application.listen(config['www_port'])
        except IOError as e:
            if e.errno in [errno.EADDRINUSE, errno.EADDRNOTAVAIL]:
                logging.info('FavourHTTPServer already running on %s:%d' % (self.ip, config['www_port']))
            else:
                raise
        finally:
            return 'http://%s:%d/' % (self.hostname, config['www_port'])


    def run(self):
        print self.hostname, self.ip, self.multicast_group

        self.io_loop = tornado.ioloop.IOLoop.instance()


        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_sock.setblocking(0)
        udp_sock.bind(self.server_address)

        stream = tornado.iostream.IOStream(udp_sock)

        listen = functools.partial(self.listen, udp_sock)
        broadcast = functools.partial(self.broadcast, udp_sock, Messages.ACK)
        drop_expired_members = functools.partial(self.__drop_expired_members, udp_sock)
        find_leader = functools.partial(self.__find_leader, udp_sock)

        periodics = [
            tornado.ioloop.PeriodicCallback(broadcast, config['broadcast_interval'] * 1000, self.io_loop),
            tornado.ioloop.PeriodicCallback(drop_expired_members, config['broadcast_interval'] * 1000, self.io_loop),
            tornado.ioloop.PeriodicCallback(find_leader, config['find_leader_every'] * 1000, self.io_loop)
        ]

        map(lambda x: x.start(), periodics)
        self.io_loop.add_handler(stream.fileno(), listen, tornado.ioloop.IOLoop.READ)

        # determine if we should launch an http interface at this location
        # test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # do_http_server = test_socket.connect_ex(('0.0.0.0', config['www_port']))
        # test_socket.close()

        self.launch_http_interface()
        self.io_loop.start()

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('hello world')


if __name__ == '__main__':
    Member(name = 'QuorumMember', args=(1,)).start()

